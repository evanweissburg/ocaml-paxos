open Core
open Async

exception RPCFailure

type replica_spec = {address: Host_and_port.t; reliable: bool; recv_disabled: bool}

val default_replica_set : ?reliable:bool -> ?num_recv_disabled:int -> unit -> replica_spec list

val num_replicas : replica_set:'a list -> int

val is_majority : replica_set:'a list -> int -> bool

val replica_of_id : replica_set:replica_spec list -> id:int -> replica_spec 

val with_rpc_conn : replica:replica_spec -> reliable:bool -> (Rpc.Connection.t -> 'a Deferred.t) -> ('a, exn) result Deferred.t
  
val with_retrying_rpc_conn : replica:replica_spec -> ?reliable:bool -> (Rpc.Connection.t -> 'a Deferred.t) -> 'a option Deferred.t