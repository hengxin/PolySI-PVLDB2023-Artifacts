#!/usr/bin/env escript
%%! -smp enable -sname erlshell -setcookie antidote

main([St]) ->
  NumDC = list_to_integer(St),
  io:format("AntidoteDB: setting up cluster for ~p datacenters!~n", [NumDC]),
  DCs = lists:map(fun(Num) -> list_to_atom(lists:flatten(io_lib:format("antidote@antidote~w", [Num]))) end, lists:seq(1, NumDC)),
  lists:foreach(fun(DC) -> rpc:call(DC, inter_dc_manager, start_bg_processes, [stable]) end, DCs),
  Descriptors = lists:map(fun(DC) -> {ok, Desc} = rpc:call(DC, inter_dc_manager, get_descriptor, []), Desc end, DCs),
  lists:foreach(fun(DC) -> rpc:call(DC, inter_dc_manager, observe_dcs_sync, [Descriptors]) end, DCs),
  io:format("Done.~n").
