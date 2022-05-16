open Core
open Async

let start_replicas ~stop ~replica_set =
  Deferred.all
    (List.init (List.length replica_set) ~f:(fun id ->
         Lib.Replica.start ~env:() ~stop ~id ~replica_set ()))
;;

let propose_values ~(replica_set : Lib.Common.replica_spec list) ~seq ~values =
  Deferred.all
    (List.mapi values ~f:(fun i proposal ->
         let replica_id = i % Lib.Common.num_replicas ~replica_set in
         let replica = Lib.Common.replica_of_id ~replica_set ~id:replica_id in
         Lib.Client.propose ~replica { seq; v = proposal }))
;;

let count_decided ~(handles : Lib.Replica.handle list) ~seq ~allowed_vals =
  let statuses = List.map handles ~f:(fun handle -> handle.status seq) in
  let filter_decided = function
    | Lib.Replica.DecidedStatus v -> Some v
    | Lib.Replica.PendingStatus | Lib.Replica.ForgottenStatus -> None
  in
  let committed = List.filter_map statuses ~f:filter_decided in
  match committed with
  | [] -> return 0
  | hd :: tl ->
    let%map () = Log.Global.flushed () in
    if not (List.exists allowed_vals ~f:(fun commit -> String.(commit = hd)))
    then failwith "Committed value not allowed!"
    else (
      let filtered = List.filter tl ~f:(fun commit -> String.(commit <> hd)) in
      if List.length filtered <> 0
      then failwith "Not all values same!"
      else List.length committed)
;;

let rec wait_majority_decided ~(handles : Lib.Replica.handle list) ~seq ~allowed_vals =
  let%bind num_decided = count_decided ~handles ~seq ~allowed_vals in
  if Lib.Common.is_majority ~replica_set:handles num_decided
  then return ()
  else (
    let%bind () = Clock.after (sec 0.1) in
    wait_majority_decided ~handles ~seq ~allowed_vals)
;;

let count_rpcs (handles : Lib.Replica.handle list) =
  List.fold handles ~init:0 ~f:(fun acc handle -> acc + handle.rpc_count ())
;;

let test_basic ~stop () =
  Log.Global.printf "TEST: test_basic";
  let replica_set = Lib.Common.default_replica_set () in
  let%bind handles = start_replicas ~stop ~replica_set in
  Log.Global.printf "Single proposer...";
  let messages = [ "a" ] in
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages in
  Log.Global.printf "Many proposers, same value...";
  let messages = [ "a"; "a"; "a" ] in
  let%bind _ = propose_values ~replica_set ~seq:1 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:1 ~allowed_vals:messages in
  Log.Global.printf "Many proposers, different values...";
  let messages = [ "a"; "b"; "c" ] in
  let%bind _ = propose_values ~replica_set ~seq:2 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:2 ~allowed_vals:messages in
  let messages = [ "a"; "b"; "c"; "d"; "e"; "f"; "g"; "h" ] in
  Log.Global.printf "Many proposals from same proposer, different values...";
  let%bind _ = propose_values ~replica_set ~seq:3 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:3 ~allowed_vals:messages in
  let messages = [ "a" ] in
  Log.Global.printf "Out of order proposals...";
  let%bind _ = propose_values ~replica_set ~seq:8 ~values:messages in
  let%bind _ = propose_values ~replica_set ~seq:7 ~values:messages in
  let%bind _ = propose_values ~replica_set ~seq:6 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:8 ~allowed_vals:messages in
  let%bind _ = propose_values ~replica_set ~seq:5 ~values:messages in
  let%bind _ = propose_values ~replica_set ~seq:4 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:7 ~allowed_vals:messages in
  let%bind () = wait_majority_decided ~handles ~seq:7 ~allowed_vals:messages in
  let%bind () = wait_majority_decided ~handles ~seq:5 ~allowed_vals:messages in
  let%bind () = wait_majority_decided ~handles ~seq:4 ~allowed_vals:messages in
  let messages = [ "a" ] in
  Log.Global.printf "Old proposal...";
  let%bind _ = propose_values ~replica_set ~seq:9 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:9 ~allowed_vals:messages in
  let%bind _ = propose_values ~replica_set ~seq:9 ~values:[ "b" ] in
  let%bind () = wait_majority_decided ~handles ~seq:9 ~allowed_vals:messages in
  return ()
;;

let test_propose_values ~stop () =
  Log.Global.printf "TEST: test_propose_values";
  let replica_set = Lib.Common.default_replica_set () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = [ "a"; "b"; "c" ] in
  let%bind commits = propose_values ~replica_set ~seq:0 ~values:messages in
  let (_ : unit list) =
    List.map commits ~f:(fun commit ->
        match commit with
        | Some v ->
          if not (List.exists messages ~f:(fun message -> String.(message = v)))
          then failwith "Non-message commit found!"
        | None -> failwith "RPC failed in unknown way")
  in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages
;;

let test_forget ~stop () =
  Log.Global.printf "TEST: test_forget";
  let replica_set = Lib.Common.default_replica_set () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = [ "a"; "b"; "c" ] in
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages in
  List.iteri handles ~f:(fun i handle -> if i = 0 then handle.set_done 0);
  let mins = List.map handles ~f:(fun handle -> handle.min ()) in
  if List.exists mins ~f:(fun min -> min <> 0) then failwith "Forgot too early!";
  List.iter handles ~f:(fun handle -> handle.set_done 0);
  let%bind _ = propose_values ~replica_set ~seq:1 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:1 ~allowed_vals:messages in
  let mins = List.map handles ~f:(fun handle -> handle.min ()) in
  if List.exists mins ~f:(fun min -> min <> 1) then failwith "Did not forget properly!";
  let statuses = List.map handles ~f:(fun handle -> handle.status 0) in
  if List.exists statuses ~f:(fun status ->
         match status with
         | Lib.Replica.ForgottenStatus -> false
         | _ -> true)
  then failwith "Status not correct after forgetting!";
  return ()
;;

let test_limp ~stop () =
  Log.Global.printf "TEST: test_limp";
  let replica_set = Lib.Common.default_replica_set ~num_recv_disabled:1 () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = [ "a"; "b" ] in
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages
;;

let test_unreliable ~stop () =
  Log.Global.printf "TEST: test_unreliable";
  let replica_set = Lib.Common.default_replica_set ~reliable:false () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = [ "a"; "b"; "c" ] in
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages
;;

let test_large_cluster_unreliable ~stop () =
  Log.Global.printf "TEST: test_large_cluster_unreliable";
  let replica_set =
    Lib.Common.
      [ { address = { host = "127.0.0.1"; port = 8765 }
        ; reliable = false
        ; recv_disabled = false
        }
      ; { address = { host = "127.0.0.1"; port = 8766 }
        ; reliable = false
        ; recv_disabled = false
        }
      ; { address = { host = "127.0.0.1"; port = 8767 }
        ; reliable = false
        ; recv_disabled = false
        }
      ; { address = { host = "127.0.0.1"; port = 8768 }
        ; reliable = false
        ; recv_disabled = false
        }
      ; { address = { host = "127.0.0.1"; port = 8769 }
        ; reliable = false
        ; recv_disabled = false
        }
      ]
  in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = [ "a"; "b"; "c" ] in
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages
;;

let test_many_unreliable ~stop () =
  Log.Global.printf "TEST: test_many_unreliable";
  let replica_set = Lib.Common.default_replica_set ~reliable:false () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = [ "a"; "b"; "c" ] in
  Deferred.List.iter ?how:(Some `Parallel) (List.init 50 ~f:Fun.id) ~f:(fun seq ->
      let%bind _ = propose_values ~replica_set ~seq ~values:messages in
      wait_majority_decided ~handles ~seq ~allowed_vals:messages)
;;

let test_efficient_1 ~stop () =
  Log.Global.printf "TEST: test_efficient_1";
  let replica_set = Lib.Common.default_replica_set () in
  let%bind handles = start_replicas ~stop ~replica_set in
  Log.Global.printf "Basic consensus takes 3 prepares, 3 accepts, 3 learns...";
  let messages = [ "a" ] in
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages in
  (* 3 prepare, 3 accept, 3 learn *)
  if count_rpcs handles <> 9 then failwith "Wrong number of RPCs issued!!";
  Log.Global.printf "Proposer adopts high n_a from single acceptor...";
  let replica =
    match replica_set with
    | [ _; _; two ] -> two
    | _ -> failwith "Impossible"
  in
  let done_info = Lib.Protocol.{ sender = 0; seq = -1 } in
  let prepare_args = Lib.Protocol.{ seq = 1; n = 1 }, done_info in
  let%bind _ =
    Lib.Common.with_rpc_conn ~replica ~reliable:true (fun conn ->
        Rpc.Rpc.dispatch_exn Lib.Protocol.prepare_rpc conn prepare_args)
  in
  let accept_args = Lib.Protocol.{ seq = 1; n = 1000; v = "b" }, done_info in
  let%bind _ =
    Lib.Common.with_rpc_conn ~replica ~reliable:true (fun conn ->
        Rpc.Rpc.dispatch_exn Lib.Protocol.accept_rpc conn accept_args)
  in
  let%bind _ = propose_values ~replica_set ~seq:1 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:1 ~allowed_vals:[ "b" ] in
  let total_rpcs =
    List.fold handles ~init:0 ~f:(fun acc handle -> acc + handle.rpc_count ())
  in
  (* 9 from earlier; then 3 prepare, 3 accept, 3 learn *)
  if total_rpcs <> 9 + 9 then failwith "Wrong number of RPCs issued!";
  Log.Global.printf "Proposer immediately decides after noticing majority...";
  let other_replicas =
    match replica_set with
    | _ :: tl -> tl
    | [] -> failwith "Impossible"
  in
  let prepare_args = Lib.Protocol.{ seq = 2; n = 2 }, done_info in
  let%bind _ =
    Deferred.all
      (List.map other_replicas ~f:(fun replica ->
           Lib.Common.with_rpc_conn ~replica ~reliable:true (fun conn ->
               Rpc.Rpc.dispatch_exn Lib.Protocol.prepare_rpc conn prepare_args)))
  in
  let accept_args = Lib.Protocol.{ seq = 2; n = 2; v = "b" }, done_info in
  let%bind _ =
    Deferred.all
      (List.map other_replicas ~f:(fun replica ->
           Lib.Common.with_rpc_conn ~replica ~reliable:true (fun conn ->
               Rpc.Rpc.dispatch_exn Lib.Protocol.accept_rpc conn accept_args)))
  in
  let%bind _ = propose_values ~replica_set ~seq:2 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:2 ~allowed_vals:[ "b" ] in
  (* 18 from earlier; then 3 prepare, 3 learn *)
  if count_rpcs handles <> 9 + 9 + 6 then failwith "Wrong number of RPCs issued!";
  return ()
;;

let test_efficient_2 ~stop () =
  Log.Global.printf "TEST: test_efficient_2";
  let replica_set = Lib.Common.default_replica_set ~num_recv_disabled:1 () in
  let%bind handles = start_replicas ~stop ~replica_set in
  Log.Global.printf "Proposer immediately decides after noticing decided acceptor...";
  let replica =
    match replica_set with
    | [ _; _; two ] -> { two with recv_disabled = false }
    | _ -> failwith "Impossible"
  in
  let messages = [ "a" ] in
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages in
  let%bind _ = Lib.Client.propose ~replica { seq = 0; v = "b" } in
  (* 9 from limp consensus; then 3 prepare, 3 learn *)
  if count_rpcs handles <> 9 + 6 then failwith "Wrong number of RPCs issued!";
  return ()
;;

let run =
  Log.Global.set_level `Info;
  let tests =
    [ test_basic
    ; test_propose_values
    ; test_limp
    ; test_forget
    ; test_unreliable
    ; test_large_cluster_unreliable
    ; test_many_unreliable
    ; test_efficient_1
    ; test_efficient_2
    ]
  in
  let runnable_tests =
    List.fold tests ~init:(return ()) ~f:(fun acc test ->
        let stop_ivar = Ivar.create () in
        let stop = Ivar.read stop_ivar in
        acc
        >>= test ~stop
        >>| Ivar.fill stop_ivar
        >>= Log.Global.flushed
        >>= fun () -> Clock.after (sec 0.1))
  in
  Log.Global.printf "Running %d tests:" (List.length tests);
  let%map _ =
    try runnable_tests with
    | err -> return (Log.Global.printf "%s" (Exn.to_string err))
  in
  Log.Global.printf "All tests passed!"
;;

let command =
  Command.async ~summary:"Paxos replica" Command.Param.(return (fun () -> run))
;;

let () = Command_unix.run command
