open Core
open Async

(* The implementation of the "hello" RPC.  The first argument is the environment
   the query executes against, which in this case is trivial.
   The RPC waits a 10th of a second before responding just to show how you do a
   query whose implementation blocks.
*)

type instance = {n_p: int ref; n_a: int option ref; v_a: string option ref; decided: bool ref}

type prepare_result =
  | MajorityExists of string
  | Supported of string
  | NotSupported of int

let instance ~instances seq = 
  match Hashtbl.find instances seq with
  | Some instance -> instance
  | None -> 
    let instance = {n_p=ref 0; n_a=ref None; v_a=ref None; decided=ref false} in
    match Hashtbl.add instances ~key:seq ~data:instance with 
    | `Duplicate | `Ok -> instance

let prepare_impl ~id ~max ~instances () (args : Protocol.prepare_args) =
  if args.seq > !max then max := args.seq else ();
  let instance = instance ~instances args.seq in 
  if args.n > !(instance.n_p) then (
    instance.n_p := args.n;
    Log.Global.debug "%d accepted prepare (n: %d)" id args.n;
    return (Protocol.PrepareOk (args.n, !(instance.n_a), !(instance.v_a)))
  ) else (
    Log.Global.debug "%d rejected prepare (n: %d)" id args.n;
    return Protocol.PrepareReject
  )

let accept_impl ~id ~max ~instances () (args : Protocol.accept_args) =
  if args.seq > !max then max := args.seq else ();
  let instance = instance ~instances args.seq in 
  if args.n >= !(instance.n_p) then (
    Log.Global.debug "%d accepted accept (n: %d, v: %s)" id args.n args.v;
    instance.n_p := args.n;
    instance.n_a := Some args.n;
    instance.v_a := Some args.v;
    instance.decided := true;
    return (Protocol.AcceptOk args.n)
  ) else (
    Log.Global.debug "%d rejected accept (n: %d, v: %s)" id args.n args.v;
    return Protocol.AcceptReject
  )

let propose_impl ~id ~max ~(replica_set:Common.replica_spec list) ~n ~instances () (args:Protocol.propose_args) =
  let num_replicas = Common.num_replicas ~replica_set in
  let inc_n n' = 
    n := (1 + n' / num_replicas) * num_replicas + id in
  let num_majority = float_of_int num_replicas /. 2. in
  let broadcast_replicas ~rpc ~local ~args = 
    List.map replica_set ~f:(fun replica ->
    let self = Common.replica_of_id ~replica_set ~id in
    if replica.port <> self.port then 
      let host, port = Common.host_port_of_replica replica in
      Common.with_rpc_conn ~host ~port ~reliable:self.reliable (fun conn -> 
        Rpc.Rpc.dispatch_exn rpc conn args)
      else try_with (fun () -> local () args)
    )
  in

  let prepare_supported results = 
    let check_support result (num_ok, max_n, max_v) =
      match result with 
      | Ok Protocol.PrepareOk (_, n, v) -> 
        (match n, v with 
          | Some n, Some v when n > max_n -> (num_ok + 1, n, v)
          | _ -> (num_ok + 1, max_n, max_v)
        )
      | Ok Protocol.PrepareReject | Error _ -> (num_ok, max_n, max_v)
    in
    let majority_value = 
      let filter_ok acc = function
        | Ok Protocol.PrepareOk (_, Some n, Some v) -> (n, v) :: acc
        | _ -> acc in
      let compare (a, _) (b, _) = a - b in
      let sorted_accepts = List.sort (List.fold results ~init:[] ~f:filter_ok) ~compare in
      let rec loop (max_n, max_v, max_count) (cur_n, cur_v, cur_count) = function
        | [] -> max_v, max_count
        | (n, v) :: tl -> 
          if n = cur_n then
            loop (max_n, max_v, max_count) (cur_n, cur_v, cur_count+1) tl
          else (
            if cur_count > max_count then 
              loop (cur_n, cur_v, cur_count) (n, v, 1) tl
            else 
              loop (max_n, max_v, max_count) (n, v, 1) tl
          ) in
      let max_v, count = loop (-1, "", 0) (-1, "", 0) sorted_accepts in
      if Float.(float_of_int count > num_majority) then Some max_v else None in
    match majority_value with 
      | Some value -> MajorityExists value
      | None -> 
        let num_supporting, max_n, max_v = (List.fold_right results ~f:check_support ~init:(0, -1, args.v)) in
        if Float.(float_of_int num_supporting > num_majority) then
          Supported max_v
        else 
          NotSupported max_n
  in 

  let accept_supported results =
    let f result aux =
      match result with 
      | Ok Protocol.AcceptOk _ -> aux + 1
      | Ok Protocol.AcceptReject | Error _ -> aux
    in 
    let num_supporting = (List.fold_right results ~f ~init:0) in
    Float.(float_of_int num_supporting > num_majority)
  in 

  let rec propose_aux () = 
    let n' = !n in
    Log.Global.debug "%d proposing %s on %d" id args.v n';
    let%bind results = Deferred.all (broadcast_replicas ~rpc:Protocol.prepare_rpc ~local:(prepare_impl ~id ~max ~instances) ~args:{seq=args.seq; n=n'}) in
    match prepare_supported results with 
      | MajorityExists value -> return value
      | NotSupported n'' ->
        inc_n (if n' > n'' then n' else n'');
        propose_aux ()
      | Supported v ->
        let%bind results = 
          Deferred.all (broadcast_replicas ~rpc:Protocol.accept_rpc ~local:(accept_impl ~id ~max ~instances) ~args:{seq=args.seq; n=n'; v=v})
        in
        if not (accept_supported results) then (
          inc_n n';
          propose_aux ()
        ) else 
          return v
  in
  propose_aux ()

(* The list of RPC implementations supported by this server *)
let implementations ~id ~replica_set ~n ~max ~instances=
  [ Rpc.Rpc.implement Protocol.prepare_rpc (prepare_impl ~id ~max ~instances);
    Rpc.Rpc.implement Protocol.accept_rpc (accept_impl ~id ~max ~instances);
    Rpc.Rpc.implement Protocol.propose_rpc (propose_impl ~id ~max ~replica_set ~n ~instances); ]

type instance_status = 
  | Decided of string
  | Pending
  | Forgotten

type handle = {min: unit -> int; max: unit -> int; status: int -> instance_status;}

let minimum ~min () = !min 

let maximum ~max () = !max

let status ~min ~instances seq = 
  if seq < !min then Forgotten else 
    match Hashtbl.find instances seq with 
    | Some instance -> (
      match !(instance.decided), !(instance.v_a) with 
      | true, Some v_a -> Decided v_a
      | true, None -> failwith "Decided but no value!"
      | false, _ -> Pending
    )
    | None -> Forgotten

let start ~env ?(stop=Deferred.never ()) ~id ~(replica_set:Common.replica_spec list) () =
  let port = (Common.replica_of_id ~id ~replica_set).port in
  Log.Global.debug "Starting server on %d" port;
  let n, min, max, instances = ref 0, ref 0, ref (-1), Hashtbl.create (module Int) in
  let implementations =
    Rpc.Implementations.create_exn ~implementations:(implementations ~id ~replica_set ~n ~max ~instances)
      ~on_unknown_rpc:(`Call (fun _ ~rpc_tag ~version ->
          Log.Global.debug "Unexpected RPC, tag %s, version %d" rpc_tag version;
          `Continue
        ))
  in
  let%map server =
    Tcp.Server.create
      ~on_handler_error:(`Call (fun _ exn -> Log.Global.sexp [%sexp (exn : Exn.t)]))
      (Tcp.Where_to_listen.of_port port)
      (fun _addr r w ->
          Rpc.Connection.server_with_close r w
            ~connection_state:(fun _ -> env)
            ~on_handshake_error:(
              `Call (fun exn -> Log.Global.sexp [%sexp (exn : Exn.t)]; return ()))
            ~implementations
      )
  in
  Log.Global.debug "Server started, waiting for close";
  Deferred.any
    [ (stop >>= fun () -> Tcp.Server.close server)
    ; Tcp.Server.close_finished server ] 
  |> Async.don't_wait_for;
  {min=minimum ~min; max=maximum ~max; status=status ~min ~instances}