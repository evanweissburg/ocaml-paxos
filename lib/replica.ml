open Core
open Async

type instance = 
  | Decided of string
  | Pending of {n_p: int ref; n_a: int option ref; v_a: string option ref}

type prepare_result =
  | WasDecided of string
  | Supported of string
  | NotSupported of int

let get_instance ~instances seq = 
  match Hashtbl.find instances seq with
  | Some instance -> instance
  | None -> 
    let instance = Pending {n_p=ref 0; n_a=ref None; v_a=ref None} in
    match Hashtbl.add instances ~key:seq ~data:instance with 
    | `Duplicate | `Ok -> instance

let decide_instance ~instances ~seq ~v =
  Log.Global.debug "Deciding on (v: %s)" v;
  let decided = Decided v in
  Hashtbl.change instances seq ~f:(fun instance -> 
    match instance with 
    | Some instance -> (
      match instance with 
        | Decided v' -> if String.(v <> v') then failwith "Instance already decided with different value!" else Some decided
        | Pending _ -> Some decided
    )
    | None -> Some decided)

let prepare_impl ~id ~(replica_set:Common.replica_spec list) ~max ~instances () (args : Protocol.prepare_args) =
  let replica = Common.replica_of_id ~replica_set ~id in 
  if replica.recv_disabled then raise Common.RPCFailure;
  if args.seq > !max then max := args.seq;
  match get_instance ~instances args.seq with
    | Decided v -> return (Protocol.PrepareDecided v)
    | Pending instance -> 
      if args.n > !(instance.n_p) then (
        instance.n_p := args.n;
        Log.Global.debug "%d accepted prepare (n: %d)" id args.n;
        return (Protocol.PrepareOk (args.n, !(instance.n_a), !(instance.v_a)))
      ) else (
        return Protocol.PrepareReject
      )

let accept_impl ~id ~(replica_set:Common.replica_spec list) ~max ~instances () (args : Protocol.accept_args) =
  let replica = Common.replica_of_id ~replica_set ~id in 
  if replica.recv_disabled then raise Common.RPCFailure;
  if args.seq > !max then max := args.seq;
  match get_instance ~instances args.seq with
    | Decided _ -> return Protocol.AcceptReject
    | Pending instance ->
      if args.n >= !(instance.n_p) then (
        Log.Global.debug "%d accepted accept (n: %d, v: %s)" id args.n args.v;
        instance.n_a := Some args.n;
        instance.v_a := Some args.v;
        return (Protocol.AcceptOk args.n)
      ) else (
        return Protocol.AcceptReject
      )

let learn_impl ~id ~(replica_set:Common.replica_spec list) ~max ~instances () (args : Protocol.learn_args) =
  Log.Global.debug "%d got learn (v: %s)" id args.v;
  let replica = Common.replica_of_id ~replica_set ~id in 
  if replica.recv_disabled then raise Common.RPCFailure else 
  if args.seq > !max then max := args.seq;
  decide_instance ~instances ~seq:args.seq ~v:args.v;
  return ()

let propose_impl ~id ~(replica_set:Common.replica_spec list) ~max ~n ~instances () (args:Protocol.propose_args) =
  let num_replicas = Common.num_replicas ~replica_set in
  let is_majority = Common.is_majority ~replica_set in 
  let inc_n n' = 
    n := (1 + n' / num_replicas) * num_replicas + id 
  in
  let broadcast_replicas ~rpc ~local ~args = 
    Deferred.all (
      List.map replica_set ~f:(fun replica ->
      let self = Common.replica_of_id ~replica_set ~id in
      if replica.port <> self.port then 
        let host, port = Common.host_port_of_replica replica in
        Common.with_rpc_conn ~host ~port ~reliable:self.reliable (fun conn -> 
          Rpc.Rpc.dispatch_exn rpc conn args)
        else try_with (fun () -> local () args)
    ))
  in

  let prepare_supported results = 
    let decided = List.filter results ~f:(fun result ->
      match result with 
        | Ok Protocol.PrepareDecided _ -> true
        | _ -> false) 
    in
    match decided with 
      | (Ok Protocol.PrepareDecided v)::_ -> WasDecided v
      | _ ->
    let check_support result (num_ok, max_n, max_v) =
      match result with 
      | Ok Protocol.PrepareOk (_, n, v) -> 
        (match n, v with 
          | Some n, Some v when n > max_n -> (num_ok + 1, n, v)
          | _ -> (num_ok + 1, max_n, max_v)
        )
      | Ok Protocol.PrepareReject | Error _ -> (num_ok, max_n, max_v)
      | Ok Protocol.PrepareDecided _ -> failwith "Impossible"
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
      if is_majority count then Some max_v else None in
    match majority_value with 
      | Some value -> WasDecided value
      | None -> 
        let num_supporting, max_n, max_v = (List.fold_right results ~f:check_support ~init:(0, -1, args.v)) in
        if is_majority num_supporting then
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
    is_majority num_supporting
  in 

  let rec propose_aux () = 
    let n' = !n in
    let local_prepare_impl = prepare_impl ~id ~replica_set ~max ~instances in
    let%bind results = broadcast_replicas ~rpc:Protocol.prepare_rpc ~local:local_prepare_impl ~args:{seq=args.seq; n=n'} in
    match prepare_supported results with 
      | WasDecided v -> 
        decide_instance ~instances ~seq:args.seq ~v;
        return v
      | NotSupported n'' ->
        inc_n (if n' > n'' then n' else n'');
        propose_aux ()
      | Supported v ->
        let local_accept_impl = accept_impl ~id ~replica_set ~max ~instances in
        let%bind results = broadcast_replicas ~rpc:Protocol.accept_rpc ~local:local_accept_impl ~args:{seq=args.seq; n=n'; v=v} in
        if not (accept_supported results) then (
          inc_n n';
          propose_aux ()
        ) else (
          let local_learn_impl = learn_impl ~id ~replica_set ~max ~instances in
          let%bind _ = broadcast_replicas ~rpc:Protocol.learn_rpc ~local:local_learn_impl ~args:{seq=args.seq; v} in
          return v
        )
  in
  propose_aux ()

(* The list of RPC implementations supported by this server *)
let implementations ~id ~replica_set ~n ~max ~instances=
  [ Rpc.Rpc.implement Protocol.prepare_rpc (prepare_impl ~id ~replica_set ~max ~instances);
    Rpc.Rpc.implement Protocol.accept_rpc (accept_impl ~id ~replica_set ~max ~instances);
    Rpc.Rpc.implement Protocol.learn_rpc (learn_impl ~id ~max ~replica_set ~instances);
    Rpc.Rpc.implement Protocol.propose_rpc (propose_impl ~id ~max ~replica_set ~n ~instances);
  ]

type instance_status = 
  | DecidedStatus of string
  | PendingStatus
  | ForgottenStatus

type handle = {min: unit -> int; max: unit -> int; status: int -> instance_status;}

let minimum ~min () = !min 

let maximum ~max () = !max

let status ~min ~instances seq = 
  if seq < !min then ForgottenStatus else 
    match Hashtbl.find instances seq with 
    | Some Decided v -> DecidedStatus v
    | Some Pending _ -> PendingStatus
    | None -> ForgottenStatus

let start ~env ?(stop=Deferred.never ()) ~id ~(replica_set:Common.replica_spec list) () =
  let port = (Common.replica_of_id ~id ~replica_set).port in
  Log.Global.debug "Starting server on %d" port;
  let n, min, max, instances = ref 0, ref 0, ref (-1), Hashtbl.create (module Int) in
  let implementations =
    Rpc.Implementations.create_exn ~implementations:(implementations ~id ~replica_set ~n ~max ~instances)
      ~on_unknown_rpc:(`Call (fun _ ~rpc_tag ~version ->
          Log.Global.error "Unexpected RPC, tag %s, version %d" rpc_tag version;
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