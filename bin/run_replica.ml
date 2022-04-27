
open Async

(* The command-line interface.  We use [async_basic] so that the command starts
   the async scheduler, and exits when the server stops.  *)
let command =
  Command.async ~summary:"Paxos replica"
  (let%map_open.Command id = flag "-id" (required int) ~doc:"int Replica's index in the (hardcoded) list of ports" in
  fun () -> 
    let replica_set = Lib.Common.default_replica_set () in
    let _ : Lib.Replica.handle Deferred.t = Lib.Replica.start ~env:() ~id ~replica_set () in 
    Deferred.never ())

let () = Command.run command