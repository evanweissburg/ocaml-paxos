
open Async

(* The command-line interface.  We use [async_basic] so that the command starts
   the async scheduler, and exits when the server stops.  *)
let id =
  let open Command.Let_syntax in
  let%map_open port =
    flag "-id" (required int) ~doc:"Replica's index in the (hardcoded) list of ports"
  in
  port

let command =
  Command.async ~summary:"Paxos replica"
    Command.Let_syntax.(
      let%map_open id = id in
      let replica_set = Lib.Common.default_replica_set () in
      fun () -> let _ : Lib.Replica.handle Deferred.t = Lib.Replica.start ~env:() ~id ~replica_set () in Async.return ())

let () = Command.run command