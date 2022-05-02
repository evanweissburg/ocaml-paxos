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

val propose_rpc : (propose_args, propose_reply) Rpc.Rpc.t

val prepare_rpc : (prepare_args * done_info, prepare_reply * done_info) Rpc.Rpc.t

val accept_rpc : (accept_args * done_info, accept_reply * done_info) Rpc.Rpc.t

val learn_rpc : (learn_args * done_info, learn_reply * done_info) Rpc.Rpc.t