open Core
open Async

let start_replicas ~stop ~replica_set  =
  Deferred.all (List.mapi replica_set ~f:(fun id -> fun _ ->
    Lib.Replica.start ~env:() ~stop ~id:id ~replica_set ()))

let propose_values ~replica_set ~seq ~values =
  Deferred.all (List.map values ~f:(fun proposal ->
    let replica = Lib.Common.replica_of_id ~replica_set ~id:(Random.int (Lib.Common.num_replicas ~replica_set)) in 
    let host, port = Lib.Common.host_port_of_replica replica in
    Lib.Client.propose ~host ~port {seq=seq; v=proposal}
   ))

let count_decided ~(handles:Lib.Replica.handle list) ~seq ~allowed_vals = 
  let statuses = List.map handles ~f:(fun handle -> handle.status seq) in 
  let filter_decided acc = function
    | Lib.Replica.DecidedStatus v -> v :: acc
    | _ -> acc in
  let committed = List.fold statuses ~init:[] ~f:filter_decided in 
  match committed with 
  | [] -> return 0 
  | hd::tl -> 
    let%map () = Log.Global.flushed () in
    if not (List.exists allowed_vals ~f:(fun commit -> String.(commit = hd))) then failwith "Commited value not allowed!" else
    let filtered = List.filter tl ~f:(fun commit -> String.(commit <> hd)) in 
    if (List.length filtered) <> 0 then failwith "Not all values same!" else
    List.length committed

let rec wait_majority_decided ~(handles:Lib.Replica.handle list) ~seq ~allowed_vals = 
  let%bind num_decided = count_decided ~handles ~seq ~allowed_vals in
  if Lib.Common.is_majority ~replica_set:handles num_decided then return ()
  else let%bind () = Clock.after (sec 0.1) in 
    wait_majority_decided ~handles ~seq ~allowed_vals

let test_basic ~stop () = 
  Log.Global.printf "TEST: test_basic";
  let replica_set = Lib.Common.default_replica_set ~reliable:true () in
  let%bind handles = start_replicas ~stop ~replica_set in

  Log.Global.printf "Single proposer...";
  let messages = ["a"] in 
  let%bind _ = propose_values ~replica_set  ~seq:0 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages in

  Log.Global.printf "Many proposers, same value...";
  let messages = ["a"; "a"; "a"] in 
  let%bind _ = propose_values ~replica_set ~seq:1 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:1 ~allowed_vals:messages in

  Log.Global.printf "Many proposers, different values...";
  let messages = ["a"; "b"; "c"] in 
  let%bind _ = propose_values ~replica_set ~seq:2 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:2 ~allowed_vals:messages in

  let messages = ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"] in 
  Log.Global.printf "Many proposals from same proposer, different values...";
  let%bind _ = propose_values ~replica_set ~seq:3 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:3 ~allowed_vals:messages in 

  let messages = ["a"] in 
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

  let messages = ["a"] in 
  Log.Global.printf "Old proposal...";
  let%bind _ = propose_values ~replica_set ~seq:9 ~values:messages in
  let%bind () = wait_majority_decided ~handles ~seq:9 ~allowed_vals:messages in 
  let%bind _ = propose_values ~replica_set ~seq:9 ~values:["b"] in
  let%bind () = wait_majority_decided ~handles ~seq:9 ~allowed_vals:messages in 

  return ()

let test_propose_values ~stop () = 
  Log.Global.printf "TEST: test_propose_values";
  let replica_set = Lib.Common.default_replica_set () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = ["a"; "b"; "c"] in 
  let%bind commits = propose_values ~replica_set ~seq:0 ~values:messages in
  let _ : unit list = List.map commits ~f:(fun commit -> 
    if not (List.exists messages ~f:(fun message -> String.(message = commit))) then 
      failwith "Non-message commit found!") in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages

let test_limp ~stop () = 
  Log.Global.printf "TEST: test_limp";
  let replica_set = Lib.Common.default_replica_set ~num_recv_disabled:1 () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = ["a"; "b"; "c"] in 
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages

let test_unreliable ~stop () =
  Log.Global.printf "TEST: test_unreliable";
  let replica_set = Lib.Common.default_replica_set ~reliable:false () in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = ["a"; "b"; "c"] in 
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages

let test_many_replicas_unreliable ~stop () =
  Log.Global.printf "TEST: test_many_replicas_unreliable";
  let replica_set = Lib.Common.([
    {host="127.0.0.1"; port=8765; reliable=false; recv_disabled=false};
    {host="127.0.0.1"; port=8766; reliable=false; recv_disabled=false};
    {host="127.0.0.1"; port=8767; reliable=false; recv_disabled=false};
    {host="127.0.0.1"; port=8768; reliable=false; recv_disabled=false};
    {host="127.0.0.1"; port=8769; reliable=false; recv_disabled=false};
  ]) in
  let%bind handles = start_replicas ~stop ~replica_set in
  let messages = ["a"; "b"; "c"] in 
  let%bind _ = propose_values ~replica_set ~seq:0 ~values:messages in
  wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages

let run = 
  Log.Global.set_level `Info;
  let tests = [
    test_basic;
    test_propose_values;
    test_limp;
    test_unreliable;
    test_many_replicas_unreliable;
  ] in
  let runnable_tests = List.fold tests ~init:(return ()) ~f:(fun acc test -> 
    let stop_ivar = Ivar.create () in
    let stop = Ivar.read stop_ivar in 
    acc >>= test ~stop >>| Ivar.fill stop_ivar >>= Log.Global.flushed >>= fun () -> Clock.after (sec 0.1)
    ) in
  Log.Global.printf "Running %d tests:" (List.length tests);
  let%map _ = try runnable_tests with err -> return (Log.Global.printf "%s" (Exn.to_string err)) in
  Log.Global.printf "All tests passed!"
  

let command =
  Command.async ~summary:"Paxos replica"
  Command.Param.(return (fun () -> run))

let () = Command.run command