open Async

type instance_status = 
  | DecidedStatus of string
  | PendingStatus
  | ForgottenStatus

type handle = {min: unit -> int; max: unit -> int; status: int -> instance_status;}

val start : env:unit -> ?stop:unit Deferred.t -> id:int -> replica_set:Common.replica_spec list -> unit -> handle Deferred.t