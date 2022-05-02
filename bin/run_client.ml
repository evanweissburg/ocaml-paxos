open Async

let command =
  Command.async
    ~summary:"Client"
      (Command.Param.return (fun () -> 
        let replica_set = Lib.Common.default_replica_set () in
        let replica = Lib.Common.replica_of_id ~replica_set ~id:0 in
        let _ : string option Deferred.t = Lib.Client.propose ~replica {seq=0; v="hello"} in 
        Deferred.never ()))

let () = Command.run command