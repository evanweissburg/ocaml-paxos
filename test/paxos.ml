open! Core
open Async

let count_decided ~(handles:Lib.Replica.handle list) ~seq = 
  let statuses = List.map handles ~f:(fun handle -> handle.status seq) in 
  let filter_decided acc = function
    | Lib.Replica.Decided v -> v :: acc
    | _ -> acc in
  let committed = List.fold statuses ~init:[] ~f:filter_decided in 
  let all_same = 
    match committed with 
    | [] -> false 
    | hd::tl -> 
      let filtered = List.filter tl ~f:(fun commit -> String.(commit <> hd)) in 
      (List.length filtered) = 0 in
  if not all_same then failwith "Not all values same!" else
  List.length committed

let%expect_test "hello" = 
  let stop_ivar = Ivar.create () in
  let stop = Ivar.read stop_ivar in
  let replica_set = Lib.Common.default_replica_set ~reliable:false () in
  let%bind _ : Lib.Replica.handle = Lib.Replica.start ~env:() ~stop ~id:0 ~replica_set () in
  let%bind _ : Lib.Replica.handle = Lib.Replica.start ~env:() ~stop ~id:1 ~replica_set () in
  let%bind _ : Lib.Replica.handle = Lib.Replica.start ~env:() ~stop ~id:2 ~replica_set () in
  let%bind commits = Deferred.all (List.map ["a"; "b"; "c"; "d"; "e"; "f"; "g"; "h"] ~f:(fun proposal ->
    let replica = Lib.Common.replica_of_id ~replica_set ~id:(Random.int (Lib.Common.num_replicas ~replica_set)) in 
    let host, port = Lib.Common.host_port_of_replica replica in
    Lib.Client.propose ~host ~port {seq=0; v=proposal}
   )) in
  Ivar.fill stop_ivar ();
  let all_same = 
    match commits with 
    | [] -> false 
    | hd::tl -> 
      let filtered = List.filter tl ~f:(fun commit -> String.(commit <> hd)) in 
      (List.length filtered) = 0 in
  printf "%b" all_same;
  let%map () = [%expect {| true |}] in
  ()