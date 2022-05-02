open Core
open Async

type propose_args = {seq: int; v: string}
  [@@deriving bin_io]

type propose_reply = string
  [@@deriving bin_io]

type done_info = {sender: int; seq: int}
  [@@deriving bin_io]

type prepare_args = {seq: int; n: int}
  [@@deriving bin_io]

type prepare_reply =
  | PrepareOk of int * int option * string option
  | PrepareDecided of string
  | PrepareReject
  [@@deriving bin_io]

type accept_args = {seq: int; n: int; v: string}
  [@@deriving bin_io]

type accept_reply =
  | AcceptOk of int
  | AcceptReject
  [@@deriving bin_io]

type learn_args = {seq: int; v: string}
  [@@deriving bin_io]

type learn_reply = unit
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
    ~bin_query:[%bin_type_class: prepare_args * done_info]
    ~bin_response:[%bin_type_class: prepare_reply * done_info]

let accept_rpc =
  Rpc.Rpc.create
    ~name:"accept"
    ~version:0
    ~bin_query:[%bin_type_class: accept_args * done_info]
    ~bin_response:[%bin_type_class: accept_reply * done_info]

let learn_rpc =
  Rpc.Rpc.create
    ~name:"learn"
    ~version:0
    ~bin_query:[%bin_type_class: learn_args * done_info]
    ~bin_response:[%bin_type_class: learn_reply * done_info]