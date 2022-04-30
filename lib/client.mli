open Async

val propose : Protocol.propose_args -> replica:Common.replica_spec -> string option Deferred.t