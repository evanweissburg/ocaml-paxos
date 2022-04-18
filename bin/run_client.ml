open! Core
open! Async

(* A command that sends the hello request  *)

let command =
  Command.async
    ~summary:"Client"
    Command.Let_syntax.(
      let%map_open host, port = 
        let replica_set = Lib.Common.default_replica_set () in
        let replica = Lib.Common.replica_of_id ~replica_set ~id:0 in
        return (Lib.Common.host_port_of_replica replica) in
      fun () -> let _ : string Deferred.t = Lib.Client.propose ~port ~host {seq=0; v="hello"} in Async.return ())


let () = Command.run command