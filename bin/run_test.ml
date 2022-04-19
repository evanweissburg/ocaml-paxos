open Core
open Async

let count_decided ~(handles:Lib.Replica.handle list) ~seq ~allowed_vals = 
  let statuses = List.map handles ~f:(fun handle -> handle.status seq) in 
  let filter_decided acc = function
    | Lib.Replica.DecidedStatus v -> v :: acc
    | _ -> acc in
  let committed = List.fold statuses ~init:[] ~f:filter_decided in 
  match committed with 
  | [] -> 0 
  | hd::tl -> 
    if not (List.exists allowed_vals ~f:(fun commit -> String.(commit = hd))) then failwith "Committed value not allowed!" else
    let filtered = List.filter tl ~f:(fun commit -> String.(commit <> hd)) in 
    if (List.length filtered) <> 0 then failwith "Not all values same!" else
    List.length committed

let rec wait_majority_decided ~(handles:Lib.Replica.handle list) ~seq ~allowed_vals = 
  let num_decided = count_decided ~handles ~seq ~allowed_vals in
  if Lib.Common.is_majority ~replica_set:handles num_decided then return ()
  else let%bind () = Clock.after (sec 0.1) in 
    wait_majority_decided ~handles ~seq ~allowed_vals

let run = 
  let stop_ivar = Ivar.create () in
  let stop = Ivar.read stop_ivar in
  let replica_set = Lib.Common.default_replica_set ~reliable:true ~recv_disabled:false () in
  let%bind handles = Deferred.all (List.map [0; 1; 2] ~f:(fun id -> Lib.Replica.start ~env:() ~stop ~id:id ~replica_set ())) in
  let messages = ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"] in 
  let%bind commits = Deferred.all (List.map messages ~f:(fun proposal ->
    let replica = Lib.Common.replica_of_id ~replica_set ~id:(Random.int (Lib.Common.num_replicas ~replica_set)) in 
    let host, port = Lib.Common.host_port_of_replica replica in
    Lib.Client.propose ~host ~port {seq=0; v=proposal}
   )) in
  let _ : unit list = List.map commits ~f:(fun commit -> Log.Global.info "Committed %s" commit) in
  let%bind () = wait_majority_decided ~handles ~seq:0 ~allowed_vals:messages in
  Ivar.fill stop_ivar ();
  let%map () = Clock.after (sec 0.1) in 
  ()

let command =
  Command.async ~summary:"Paxos replica"
  Command.Param.(return (fun () -> run))

let () = Command.run command