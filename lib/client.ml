open Async

let propose message ~replica =
  Common.with_retrying_rpc_conn ~replica (fun conn ->
      Rpc.Rpc.dispatch_exn Protocol.propose_rpc conn message)
;;
