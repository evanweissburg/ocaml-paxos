open Core
open Async

(* The command-line interface.  We use [async_basic] so that the command starts
   the async scheduler, and exits when the server stops.  *)

let run = 
  let stop_ivar = Ivar.create () in
  let stop = Ivar.read stop_ivar in
  let replica_set = Lib.Common.default_replica_set ~reliable:false () in
  let%bind _ : Lib.Replica.handle = Lib.Replica.start ~env:() ~stop ~id:0 ~replica_set () in
  let%bind _ : Lib.Replica.handle = Lib.Replica.start ~env:() ~stop ~id:1 ~replica_set () in
  let%bind _ : Lib.Replica.handle = Lib.Replica.start ~env:() ~stop ~id:2 ~replica_set () in
  let%bind commits = Deferred.all (List.map ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"] ~f:(fun proposal ->
    let replica = Lib.Common.replica_of_id ~replica_set ~id:(Random.int (Lib.Common.num_replicas ~replica_set)) in 
    let host, port = Lib.Common.host_port_of_replica replica in
    Lib.Client.propose ~host ~port {seq=0; v=proposal}
   )) in
  let _ : unit list = List.map commits ~f:(fun commit -> Log.Global.info "Committed %s" commit) in
  let () = Ivar.fill stop_ivar () in
  return ()

let command =
  Command.async ~summary:"Paxos replica"
  Command.Param.(return (fun () -> run))

let () = Command.run command