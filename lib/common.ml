open Core
open Async

type replica_spec = {host: string; port: int; reliable: bool}

let default_replica_set ?(reliable=false) () = 
  [
    {host="127.0.0.1"; port=8765; reliable};
    {host="127.0.0.1"; port=8766; reliable};
    {host="127.0.0.1"; port=8767; reliable};
  ]
let num_replicas ~replica_set = List.length replica_set
let replica_of_id ~replica_set ~id = 
  match List.nth replica_set id with 
  | Some replica -> replica
  | None -> failwith (Printf.sprintf "Replica id %d too large for n = %d" id (List.length replica_set))

let host_port_of_replica replica =
  replica.host, replica.port

let with_rpc_conn ~host ~port ~reliable f =
  try_with (fun () ->
  let rand_num = Random.int 100 in
  if not reliable && rand_num < 40 then failwith "Faulty forwards RPC error"
  else 
  Tcp.with_connection
    (Tcp.Where_to_connect.of_host_and_port
       (Host_and_port.create ~host ~port))
    ~timeout:(sec 1.)
    (fun _ r w ->
       match%bind Rpc.Connection.create r w ~connection_state:(fun _ -> ()) with
       | Error exn -> raise exn
       | Ok conn   -> 
        if not reliable && rand_num < 80 then failwith "Faulty backwards RPC error"
        else f conn
    )
  )
  
let rec with_retrying_rpc_conn ~host ~port ?(reliable=true) f =
  match%bind with_rpc_conn ~host ~port ~reliable f with
  | Ok reply -> return reply
  | Error _ -> with_retrying_rpc_conn ~host ~port ~reliable f
