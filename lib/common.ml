open Core
open Async

exception RPCFailure

type replica_spec = {address: Host_and_port.t; reliable: bool; recv_disabled: bool}

let default_replica_set ?(reliable=true) ?(num_recv_disabled=0) () = 
  [
    {address={host="127.0.0.1"; port=8765}; reliable; recv_disabled=(num_recv_disabled >= 3)};
    {address={host="127.0.0.1"; port=8766}; reliable; recv_disabled=(num_recv_disabled >= 2)};
    {address={host="127.0.0.1"; port=8767}; reliable; recv_disabled=(num_recv_disabled >= 1)};
  ]
let num_replicas ~replica_set = List.length replica_set

let is_majority ~replica_set n = 
  let float_n = float_of_int n in 
  let float_majority = float_of_int (num_replicas ~replica_set) /. 2. in
  Float.(float_n > float_majority)

let replica_of_id ~replica_set ~id = 
  match List.nth replica_set id with 
  | Some replica -> replica
  | None -> failwith (Printf.sprintf "Replica id %d too large for n = %d" id (List.length replica_set))

let with_rpc_conn ~address ~reliable f =
  try_with (fun () ->
  let rand_num = Random.int 100 in
  if not reliable && rand_num < 10 then raise RPCFailure
  else 
  Tcp.with_connection
    (Tcp.Where_to_connect.of_host_and_port address)
    ~timeout:(sec 1.)
    (fun _ r w ->
       match%bind Rpc.Connection.create r w ~connection_state:(fun _ -> ()) with
       | Error exn -> raise exn
       | Ok conn  -> 
        if not reliable && rand_num < 20 then raise RPCFailure
        else f conn
    )
  )
  
let rec with_retrying_rpc_conn ~address ?(reliable=true) f =
  match%bind with_rpc_conn ~address ~reliable f with
  | Ok reply -> return (Some reply)
  | Error RPCFailure -> 
    let%bind () = Clock.after (sec 0.1) in
    with_retrying_rpc_conn ~address ~reliable f
  | Error err -> 
    Log.Global.error "%s" (Exn.to_string err);
    return None
