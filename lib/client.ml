open! Core
open! Async

let propose message ~address =
  Common.with_retrying_rpc_conn ~address (fun conn ->
    Rpc.Rpc.dispatch_exn Protocol.propose_rpc conn message);
  