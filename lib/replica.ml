open Core
open Async

let proposer_delay = sec 0.1

type instance =
  | Decided of string
  | Pending of
      { n_p : int ref
      ; n_a : int option ref
      ; v_a : string option ref
      }

let get_instance ~instances seq =
  match Hashtbl.find instances seq with
  | Some instance -> instance
  | None ->
    let instance = Pending { n_p = ref 0; n_a = ref None; v_a = ref None } in
    (match Hashtbl.add instances ~key:seq ~data:instance with
    | `Duplicate | `Ok -> instance)
;;

let decide_instance ~instances ~seq ~v =
  Log.Global.debug "Deciding on (v: %s)" v;
  let decided = Decided v in
  Hashtbl.change instances seq ~f:(fun instance ->
      match instance with
      | Some instance ->
        (match instance with
        | Decided v' ->
          if String.(v <> v')
          then assert false (* Instance already decided with different value *)
          else Some decided
        | Pending _ -> Some decided)
      | None -> Some decided)
;;

let get_done_info ~done_estimates ~id =
  match Hashtbl.find done_estimates id with
  | Some estimate -> Protocol.{ sender = id; seq = estimate }
  | None -> Protocol.{ sender = id; seq = -1 }
;;

let set_done ~done_estimates ~min ~instances ~id seq =
  Hashtbl.change done_estimates id ~f:(fun estimate ->
      match estimate with
      | Some estimate -> Some (if seq > estimate then seq else estimate)
      | None -> assert false (* Done estimates should be pre-initialized *));
  let min_done_estimate =
    Hashtbl.fold done_estimates ~init:seq ~f:(fun ~key ~data min ->
        ignore key;
        if data < min then data else min)
  in
  let rec forget () =
    if !min <= min_done_estimate
    then (
      Hashtbl.remove instances !min;
      min := !min + 1;
      forget ())
    else ()
  in
  forget ()
;;

let prepare_impl
    ~id
    ~min
    ~max
    ~instances
    ~done_estimates
    ()
    ((args, args_done_info) : Protocol.prepare_args * Protocol.done_info)
  =
  set_done ~done_estimates ~min ~instances ~id:args_done_info.sender args_done_info.seq;
  let done_info = get_done_info ~done_estimates ~id in
  if args.seq > !max then max := args.seq;
  match get_instance ~instances args.seq with
  | Decided v -> return (Protocol.PrepareDecided v, done_info)
  | Pending instance ->
    if args.n > !(instance.n_p)
    then (
      instance.n_p := args.n;
      Log.Global.debug "%d accepted prepare (n: %d)" id args.n;
      return (Protocol.PrepareOk (args.n, !(instance.n_a), !(instance.v_a)), done_info))
    else return (Protocol.PrepareReject, done_info)
;;

let accept_impl
    ~id
    ~min
    ~max
    ~instances
    ~done_estimates
    ()
    ((args, args_done_info) : Protocol.accept_args * Protocol.done_info)
  =
  set_done ~done_estimates ~min ~instances ~id:args_done_info.sender args_done_info.seq;
  let done_info = get_done_info ~done_estimates ~id in
  if args.seq > !max then max := args.seq;
  match get_instance ~instances args.seq with
  | Decided _ -> return (Protocol.AcceptReject, done_info)
  | Pending instance ->
    if args.n >= !(instance.n_p)
    then (
      Log.Global.debug "%d accepted accept (n: %d, v: %s)" id args.n args.v;
      instance.n_a := Some args.n;
      instance.v_a := Some args.v;
      return (Protocol.AcceptOk args.n, done_info))
    else return (Protocol.AcceptReject, done_info)
;;

let learn_impl
    ~id
    ~min
    ~max
    ~instances
    ~done_estimates
    ()
    ((args, args_done_info) : Protocol.learn_args * Protocol.done_info)
  =
  Log.Global.debug "%d got learn (v: %s)" id args.v;
  set_done ~done_estimates ~min ~instances ~id:args_done_info.sender args_done_info.seq;
  let done_info = get_done_info ~done_estimates ~id in
  if args.seq > !max then max := args.seq;
  decide_instance ~instances ~seq:args.seq ~v:args.v;
  return ((), done_info)
;;

let propose_impl
    ~id
    ~(replica_set : Common.replica_spec list)
    ~min
    ~max
    ~n
    ~instances
    ~done_estimates
    ~rpc_counter
    ()
    (args : Protocol.propose_args)
  =
  let is_majority = Common.is_majority ~replica_set in
  let prepare_supported results =
    let decided =
      List.reduce results ~f:(fun acc result ->
          match result with
          | Protocol.PrepareDecided v -> Protocol.PrepareDecided v
          | Protocol.PrepareOk _ | Protocol.PrepareReject -> acc)
    in
    match decided with
    | Some (Protocol.PrepareDecided v) -> `WasDecided v
    | Some (Protocol.PrepareOk _ | Protocol.PrepareReject) | None ->
      let check_support result (num_ok, max_n, max_v) =
        match result with
        | Protocol.PrepareOk (_, n, v) ->
          (match n, v with
          | Some n, Some v when n > max_n -> num_ok + 1, n, v
          | Some _, Some _ | None, None -> num_ok + 1, max_n, max_v
          | Some _, None -> assert false (* n_a and v_a are set together *)
          | None, Some _ -> assert false (* n_a and v_a are set together *))
        | Protocol.PrepareReject -> num_ok, max_n, max_v
        | Protocol.PrepareDecided _ -> assert false (* already checked for any decided *)
      in
      let majority_value =
        let sorted_accepts =
          let extract_accept = function
            | Protocol.PrepareOk (_, Some n, Some v) -> Some (n, v)
            | Protocol.PrepareOk (_, _, _)
            | Protocol.PrepareReject | Protocol.PrepareDecided _ -> None
          in
          let compare (a, _) (b, _) = a - b in
          List.filter_map results ~f:extract_accept |> List.sort ~compare
        in
        let rec loop (max_n, max_v, max_count) (cur_n, cur_v, cur_count) = function
          | [] -> if cur_count > max_count then cur_v, cur_count else max_v, max_count
          | (n, v) :: tl ->
            if n = cur_n
            then loop (max_n, max_v, max_count) (cur_n, cur_v, cur_count + 1) tl
            else if cur_count > max_count
            then loop (cur_n, cur_v, cur_count) (n, v, 1) tl
            else loop (max_n, max_v, max_count) (n, v, 1) tl
        in
        let max_v, count = loop (-1, "", 0) (-1, "", 0) sorted_accepts in
        if is_majority count then Some max_v else None
      in
      (match majority_value with
      | Some value -> `WasDecided value
      | None ->
        let num_supporting, max_n, max_v =
          List.fold_right results ~f:check_support ~init:(0, -1, args.v)
        in
        if is_majority num_supporting then `Supported max_v else `NotSupported max_n)
  in
  let accept_supported results =
    let num_supporting =
      List.sum
        (module Int)
        results
        ~f:(function
          | Protocol.AcceptOk _ -> 1
          | Protocol.AcceptReject -> 0)
    in
    is_majority num_supporting
  in
  let inc_n n' =
    let num_replicas = Common.num_replicas ~replica_set in
    n := ((1 + (n' / num_replicas)) * num_replicas) + id
  in
  let sync_n_with_local_acceptor () =
    match get_instance ~instances args.seq with
    | Decided v -> `Decided v
    | Pending instance ->
      inc_n !(instance.n_p);
      `Pending !n
  in
  let local_prepare_impl = prepare_impl ~id ~min ~max ~instances ~done_estimates in
  let local_accept_impl = accept_impl ~id ~min ~max ~instances ~done_estimates in
  let local_learn_impl = learn_impl ~id ~min ~max ~instances ~done_estimates in
  let apply_done_info results =
    let filter_ok results =
      List.fold results ~init:[] ~f:(fun acc result ->
          match result with
          | Ok result -> result :: acc
          | Error _ -> acc)
    in
    let set_done_infos results =
      List.map results ~f:(fun (result, (done_info : Protocol.done_info)) ->
          set_done ~done_estimates ~min ~instances ~id:done_info.sender done_info.seq;
          result)
    in
    results |> filter_ok |> set_done_infos
  in
  let broadcast_replicas ~rpc ~local ~args =
    Deferred.all
      (List.map replica_set ~f:(fun replica ->
           rpc_counter := !rpc_counter + 1;
           let self = Common.replica_of_id ~replica_set ~id in
           if Host_and_port.(replica.address <> self.address)
           then
             Common.with_rpc_conn ~replica ~reliable:self.reliable (fun conn ->
                 Rpc.Rpc.dispatch_exn rpc conn args)
           else try_with (fun () -> local () args)))
    >>| apply_done_info
  in
  let rec propose_aux () =
    match sync_n_with_local_acceptor () with
    | `Decided v -> return v
    | `Pending n ->
      let done_info = get_done_info ~done_estimates ~id in
      let prepare_args = Protocol.{ seq = args.seq; n }, done_info in
      let%bind results =
        broadcast_replicas
          ~rpc:Protocol.prepare_rpc
          ~local:local_prepare_impl
          ~args:prepare_args
      in
      (match prepare_supported results with
      | `WasDecided v ->
        let done_info = get_done_info ~done_estimates ~id in
        let learn_args = Protocol.{ seq = args.seq; v }, done_info in
        let%bind _ =
          broadcast_replicas
            ~rpc:Protocol.learn_rpc
            ~local:local_learn_impl
            ~args:learn_args
        in
        return v
      | `NotSupported n' ->
        inc_n (if n > n' then n else n');
        let%bind () = Clock.after proposer_delay in
        propose_aux ()
      | `Supported v ->
        let done_info = get_done_info ~done_estimates ~id in
        let accept_args = Protocol.{ seq = args.seq; n; v }, done_info in
        let%bind results =
          broadcast_replicas
            ~rpc:Protocol.accept_rpc
            ~local:local_accept_impl
            ~args:accept_args
        in
        if not (accept_supported results)
        then (
          inc_n n;
          let%bind () = Clock.after proposer_delay in
          propose_aux ())
        else (
          let done_info = get_done_info ~done_estimates ~id in
          let learn_args = Protocol.{ seq = args.seq; v }, done_info in
          let%bind _ =
            broadcast_replicas
              ~rpc:Protocol.learn_rpc
              ~local:local_learn_impl
              ~args:learn_args
          in
          return v))
  in
  propose_aux ()
;;

(* The list of RPC implementations supported by this server *)
let implementations ~id ~replica_set ~n ~min ~max ~instances ~done_estimates ~rpc_counter =
  [ Rpc.Rpc.implement
      Protocol.prepare_rpc
      (prepare_impl ~id ~min ~max ~instances ~done_estimates)
  ; Rpc.Rpc.implement
      Protocol.accept_rpc
      (accept_impl ~id ~min ~max ~instances ~done_estimates)
  ; Rpc.Rpc.implement
      Protocol.learn_rpc
      (learn_impl ~id ~min ~max ~instances ~done_estimates)
  ; Rpc.Rpc.implement
      Protocol.propose_rpc
      (propose_impl ~id ~min ~max ~replica_set ~n ~instances ~done_estimates ~rpc_counter)
  ]
;;

type instance_status =
  | DecidedStatus of string
  | PendingStatus
  | ForgottenStatus

let minimum ~min () = !min
let maximum ~max () = !max

let status ~min ~instances seq =
  if seq < !min
  then ForgottenStatus
  else (
    match Hashtbl.find instances seq with
    | Some (Decided v) -> DecidedStatus v
    | Some (Pending _) -> PendingStatus
    | None -> ForgottenStatus)
;;

let rpc_count ~rpc_counter () = !rpc_counter

type handle =
  { min : unit -> int
  ; max : unit -> int
  ; status : int -> instance_status
  ; rpc_count : unit -> int
  ; set_done : int -> unit
  }

let start
    ~env
    ?(stop = Deferred.never ())
    ~id
    ~(replica_set : Common.replica_spec list)
    ()
  =
  let port = (Common.replica_of_id ~id ~replica_set).address.port in
  Log.Global.debug "Starting server on %d" port;
  let n, min, max = ref 0, ref 0, ref 0 in
  let instances, done_estimates =
    Hashtbl.create (module Int), Hashtbl.create (module Int)
  in
  List.iteri replica_set ~f:(fun i _ ->
      Hashtbl.change done_estimates i ~f:(fun _ -> Some (-1)));
  let rpc_counter = ref 0 in
  let implementations =
    Rpc.Implementations.create_exn
      ~implementations:
        (implementations
           ~id
           ~replica_set
           ~n
           ~min
           ~max
           ~instances
           ~done_estimates
           ~rpc_counter)
      ~on_unknown_rpc:
        (`Call
          (fun _ ~rpc_tag ~version ->
            Log.Global.error "Unexpected RPC, tag %s, version %d" rpc_tag version;
            `Continue))
  in
  let%map server =
    Tcp.Server.create
      ~on_handler_error:(`Call (fun _ exn -> Log.Global.sexp [%sexp (exn : Exn.t)]))
      (Tcp.Where_to_listen.of_port port)
      (fun _addr r w ->
        Rpc.Connection.server_with_close
          r
          w
          ~connection_state:(fun _ -> env)
          ~on_handshake_error:
            (`Call
              (fun exn ->
                Log.Global.sexp [%sexp (exn : Exn.t)];
                return ()))
          ~implementations)
  in
  Log.Global.debug "Server started, waiting for close";
  Deferred.any
    [ (stop >>= fun () -> Tcp.Server.close server); Tcp.Server.close_finished server ]
  |> Async.don't_wait_for;
  { min = minimum ~min
  ; max = maximum ~max
  ; status = status ~min ~instances
  ; rpc_count = rpc_count ~rpc_counter
  ; set_done = set_done ~done_estimates ~min ~instances ~id
  }
;;
