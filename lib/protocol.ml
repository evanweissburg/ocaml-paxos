open Core
open Async

(** The protocol for communicating between the hello client and server.
    There's a single RPC call exposed, which lets you send and receive a
    string.
    The [bin_query] and [bin_response] arguments are values that contain logic
    for binary serialization of the query and response types, in this case,
    both strings.
    The version number is used when you want to mint new versions of an RPC
    without disturbing older versions.
*)

type propose_args = {seq: int; v: string}
  [@@deriving bin_io]

type propose_reply = string
  [@@deriving bin_io]

type prepare_args = {seq: int; n: int}
  [@@deriving bin_io]

type prepare_reply =
  | PrepareOk of int * int option * string option
  | PrepareReject
  [@@deriving bin_io]

type accept_args = {seq: int; n: int; v: string}
  [@@deriving bin_io]

type accept_reply =
  | AcceptOk of int
  | AcceptReject
  [@@deriving bin_io]

let propose_rpc =
  Rpc.Rpc.create
    ~name:"propose"
    ~version:0
    ~bin_query:[%bin_type_class: propose_args]
    ~bin_response:[%bin_type_class: propose_reply]

let prepare_rpc =
  Rpc.Rpc.create
    ~name:"prepare"
    ~version:0
    ~bin_query:[%bin_type_class: prepare_args]
    ~bin_response:[%bin_type_class: prepare_reply]

let accept_rpc =
  Rpc.Rpc.create
    ~name:"accept"
    ~version:0
    ~bin_query:[%bin_type_class: accept_args]
    ~bin_response:[%bin_type_class: accept_reply]