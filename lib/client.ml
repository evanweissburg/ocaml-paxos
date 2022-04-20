open! Core
open! Async

(* A command that sends the hello request  *)
let propose message ~host ~port =
  Common.with_retrying_rpc_conn ~host ~port (fun conn ->
    Rpc.Rpc.dispatch_exn Protocol.propose_rpc conn message);
  