%% -------------------------------------------------------------------
%%
%% riakc_obj: Container for Riak data and metadata
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc riakc_obj is used to wrap bucket/key/value data sent to the
%%                server on put and received on get.  It provides
%%                accessors for retrieving the data and metadata
%%                and copes with siblings if multiple values are allowed.


-module(riakc_obj).

-include("riakc_obj.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type key() :: binary().
-type bucket() :: binary().
-type metadata() :: dict().
-type value() :: binary().
-type content_type() :: string().

-define(MAX_KEY_SIZE, 65536).

-record(r_content, {
          metadata :: dict(),
          value :: term()
         }).

%% Opaque container for Riak objects, a.k.a. riak_object()
-record(r_object, {
          bucket :: bucket(),
          key :: key(),
          contents :: [#r_content{}],
          vclock :: vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).
-type riak_object() :: #r_object{}.

-type vclock() :: [vc_entry()].
% The timestamp is present but not used, in case a client wishes to inspect it.
-type vc_entry() :: {vclock_node(), {counter(), timestamp()}}.

% Nodes can have any term() as a name, but they must differ from each other.
-type   vclock_node() :: term().
-type   counter() :: integer().
-type   timestamp() :: integer().


-export([new/2, new/3, new/4,
         bucket/1,
         key/1,
         vclock/1,
         value_count/1,
         select_sibling/2,
         get_contents/1,
         get_metadata/1,
         get_metadatas/1,
         get_content_type/1,
         get_content_types/1,
         get_value/1,
         get_values/1,
         update_metadata/2,
         update_value/2,
         update_value/3,
         update_content_type/2,
         get_update_metadata/1,
         get_update_content_type/1,
         get_update_value/1,
         md_ctype/1,
				 apply_updates/1
        ]).
%% Internal library use only
-export([new_obj/4]).

%% @doc Constructor for new riak client objects.
-spec new(riak_object:bucket(), riak_object:key()) -> #r_object{}.
new(Bucket, Key) ->
    #r_object{bucket = Bucket, key = Key, contents = []}.

%% @doc  INTERNAL USE ONLY.  Set the contents of riak_object to the
%%       {Metadata, Value} pairs in MVs. Normal clients should use the
%%       set_update_[value|metadata]() + apply_updates() method for changing
%%       object contents.
%% @private
-spec new_obj(riak_object:bucket(), riak_object:key(), vclock(), #r_content{}) -> #r_object{}.
new_obj(Bucket, Key, Vclock, Contents) ->
    #r_object{bucket = Bucket, key = Key, vclock = Vclock, contents = Contents}.

%% @doc Return the content type of the value if there are no siblings
-spec get_content_type(#r_object{}) -> riak_object:content_type().
get_content_type(Object=#r_object{}) ->
    UM = riakc_obj:get_metadata(Object),
    md_ctype(UM).

%% @doc Return a list of content types for all siblings
-spec get_content_types(#r_object{}) -> [riak_object:content_type()].
get_content_types(Object=#r_object{}) ->
    F = fun({M,_}) -> md_ctype(M) end,
    [F(C) || C<- riakc_obj:get_contents(Object)].

%% @doc  Set the updated content-type of an object to CT.
-spec update_content_type(#r_object{}, content_type()) -> #r_object{}.
update_content_type(Object=#r_object{}, CT) ->
    M1 = get_update_metadata(Object),
		update_metadata(Object, dict:store(?MD_CTYPE, CT, M1)).

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       metadata.  This function will throw siblings if the object has 
%%       siblings (value_count() > 1).
-spec get_metadata(#r_object{}) -> metadata().
get_metadata(O=#r_object{}) ->
    case get_contents(O) of
        [] ->
            throw(no_metadata);
        [{MD,_V}] ->
            MD;
        _ ->
            throw(siblings)
    end.

%% @doc  Return the updated metadata of this riakc_obj.
-spec get_update_metadata(#r_object{}) -> metadata().
get_update_metadata(#r_object{updatemetadata=UM}=Object) ->
    case dict:find(clean, UM) of
        {ok, true} ->
            try
                get_metadata(Object)
            catch 
                throw:no_metadata ->
                    dict:new()
            end;
        _ ->
            UM
    end.

%% @doc Return the content type of the update value
get_update_content_type(Object=#r_object{}) ->
    UM = get_update_metadata(Object),
    md_ctype(UM).

%% @doc  Set the updated value of an object to V
-spec update_value(#r_object{}, value(), content_type()) -> #r_object{}.
update_value(Object=#r_object{}, V, CT) -> 
    O1 = update_content_type(Object, CT),
    O1#r_object{updatevalue=V}.

%% @doc  Assert that this riakc_obj has no siblings and return its associated
%%       value.  This function will throw siblings if the object has 
%%       siblings (value_count() > 1).
-spec get_value(#r_object{}) -> term().
get_value(#r_object{}=O) ->
    case get_contents(O) of
        [] ->
            throw(no_value);
        [{_MD,V}] ->
            V;
        _ ->
            throw(siblings)
    end.

%% @doc  Return the updated value of this riakc_obj.
-spec get_update_value(#r_object{}) -> value().
get_update_value(#r_object{updatevalue=UV}=Object) -> 
    case UV of
        undefined ->
            get_value(Object);
        UV ->
            UV
    end.


%% @doc  Return the content type from metadata
-spec md_ctype(dict()) -> undefined | riak_object:content_type().
md_ctype(MetaData) ->
    case dict:find(?MD_CTYPE, MetaData) of
        error ->
            undefined;
        {ok, Ctype} ->
            Ctype
    end.

%% @doc  Select the sibling to use for update - starting from 1.
-spec select_sibling(pos_integer(), #r_object{}) -> #r_object{}.
select_sibling(Index, O) ->
    Contents = lists:nth(Index, O#r_object.contents),
		O#r_object{updatemetadata=Contents#r_content.metadata, updatevalue=Contents#r_content.value}.

%% @doc Constructor for new riak objects.
-spec new(Bucket::bucket(), Key::key(), Value::value()) -> riak_object().
new(B, K, V) when is_binary(B), is_binary(K) ->
    new(B, K, V, no_initial_metadata).

%% @doc Constructor for new riak objects with an initial content-type.
-spec new(Bucket::bucket(), Key::key(), Value::value(), string() | dict()) -> riak_object().
new(B, K, V, C) when is_binary(B), is_binary(K), is_list(C) ->
    new(B, K, V, dict:from_list([{?MD_CTYPE, C}]));

%% @doc Constructor for new riak objects with an initial metadata dict.
%%
%% NOTE: Removed "is_tuple(MD)" guard to make Dialyzer happy.  The previous clause
%%       has a guard for string(), so this clause is OK without the guard.
new(B, K, V, MD) when is_binary(B), is_binary(K) ->
    case size(K) > ?MAX_KEY_SIZE of
        true ->
            throw({error,key_too_large});
        false ->
            case MD of
                no_initial_metadata -> 
                    Contents = [#r_content{metadata=dict:new(), value=V}],
                    #r_object{bucket=B,key=K,
                              contents=Contents,vclock=[]};
                _ ->
                    Contents = [#r_content{metadata=MD, value=V}],
                    #r_object{bucket=B,key=K,updatemetadata=MD,
                              contents=Contents,vclock=[]}
            end
    end.

%% @spec bucket(riak_object()) -> bucket()
%% @doc Return the containing bucket for this riak_object.
bucket(#r_object{bucket=Bucket}) -> Bucket.

%% @spec get_metadatas(riak_object()) -> [dict()]
%% @doc  Return a list of the metadata values for this riak_object.
get_metadatas(#r_object{contents=Contents}) ->
    [Content#r_content.metadata || Content <- Contents].

%% @spec key(riak_object()) -> key()
%% @doc  Return the key for this riak_object.
key(#r_object{key=Key}) -> Key.

%% @spec update_value(riak_object(), value()) -> riak_object()
%% @doc  Set the updated value of an object to V
update_value(Object=#r_object{}, V) -> Object#r_object{updatevalue=V}.

%% @spec value_count(riak_object()) -> non_neg_integer()
%% @doc  Return the number of values (siblings) of this riak_object.
value_count(#r_object{contents=Contents}) -> length(Contents).

%% @spec vclock(riak_object()) -> vclock()
%% @doc  Return the vector clock for this riak_object.
vclock(#r_object{vclock=VClock}) -> VClock.

%% @spec get_values(riak_object()) -> [value()]
%% @doc  Return a list of object values for this riak_object.
get_values(#r_object{contents=C}) -> [Content#r_content.value || Content <- C].

%% @spec get_contents(riak_object()) -> [{dict(), value()}]
%% @doc  Return the contents (a list of {metadata, value} tuples) for
%%       this riak_object.
get_contents(#r_object{contents=Contents}) ->
    [{Content#r_content.metadata, Content#r_content.value} ||
        Content <- Contents].

%% @spec update_metadata(riak_object(), dict()) -> riak_object()
%% @doc  Set the updated metadata of an object to M.
update_metadata(Object=#r_object{}, M) ->
    Object#r_object{updatemetadata=dict:erase(clean, M)}.

%% @spec apply_updates(riak_object()) -> riak_object()
%% @doc  Promote pending updates (made with the update_value() and
%%       update_metadata() calls) to this riak_object.
apply_updates(Object=#r_object{}) ->
    VL = case Object#r_object.updatevalue of
             undefined ->
                 [C#r_content.value || C <- Object#r_object.contents];
             _ ->
                 [Object#r_object.updatevalue]
         end,
    MD = case dict:find(clean, Object#r_object.updatemetadata) of
             {ok,_} ->
                 MDs = [C#r_content.metadata || C <- Object#r_object.contents],
                 case Object#r_object.updatevalue of
                     undefined -> MDs;
                     _ -> [hd(MDs)]
                 end;
             error ->
                 [dict:erase(clean,Object#r_object.updatemetadata) || _X <- VL]
         end,
    Contents = [#r_content{metadata=M,value=V} || {M,V} <- lists:zip(MD, VL)],
    Object#r_object{contents=Contents,
                 updatemetadata=dict:store(clean, true, dict:new()),
                 updatevalue=undefined}.

%% ===================================================================
%% Unit Tests
%% ===================================================================
-ifdef(TEST).

bucket_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertEqual(<<"b">>, bucket(O)).

key_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertEqual(<<"k">>, key(O)).

vclock_test() ->
    %% For internal use only
    O = new_obj(<<"b">>, <<"k">>, <<"vclock">>, []),
    ?assertEqual(<<"vclock">>, vclock(O)).

newcontent0_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertEqual(0, value_count(O)),
    ?assertEqual([], get_metadatas(O)),
    ?assertEqual([], get_values(O)),
    ?assertEqual([], get_contents(O)),
    ?assertThrow(no_metadata, get_metadata(O)),
    ?assertThrow(no_value, get_value(O)).    

contents0_test() ->
    O = new_obj(<<"b">>, <<"k">>, <<"vclock">>, []),
    ?assertEqual(0, value_count(O)),
    ?assertEqual([], get_metadatas(O)),
    ?assertEqual([], get_values(O)),
    ?assertEqual([], get_contents(O)),
    ?assertThrow(no_metadata, get_metadata(O)),
    ?assertThrow(no_value, get_value(O)).

contents1_test() ->
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O = new_obj(<<"b">>, <<"k">>, <<"vclock">>,
                      [#r_content{metadata = M1, value = <<"val1">>}]),
    ?assertEqual(1, value_count(O)),
    ?assertEqual([M1], get_metadatas(O)),
    ?assertEqual([<<"val1">>], get_values(O)),
    ?assertEqual([{M1,<<"val1">>}], get_contents(O)),
    ?assertEqual(M1, get_metadata(O)),
    ?assertEqual(<<"val1">>, get_value(O)).

contents2_test() ->
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    M2 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O = new_obj(<<"b">>, <<"k">>, <<"vclock">>,
                      [#r_content{metadata = M1, value = <<"val1">>},
											 #r_content{metadata = M2, value = <<"val2">>}]),
    ?assertEqual(2, value_count(O)),
    ?assertEqual([M1, M2], get_metadatas(O)),
    ?assertEqual([<<"val1">>, <<"val2">>], get_values(O)),
    ?assertEqual([{M1,<<"val1">>},{M2,<<"val2">>}], get_contents(O)),
    ?assertThrow(siblings, get_metadata(O)),
    ?assertThrow(siblings, get_value(O)).

update_metadata_test() ->
    O = new(<<"b">>, <<"k">>),
    UM = get_update_metadata(O),
    ?assertEqual([], dict:to_list(UM)).

update_value_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertThrow(no_value, get_update_value(O)),
    O1 = update_value(O, <<"v">>),
    ?assertEqual(<<"v">>, get_update_value(O1)),
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O2 = update_metadata(O1, M1),
    ?assertEqual(M1, get_update_metadata(O2)).

updatevalue_ct_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertThrow(no_value, get_update_value(O)),
    O1 = update_value(O, <<"v">>, "x-application/custom"),
    ?assertEqual(<<"v">>, get_update_value(O1)),
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O2 = update_metadata(O1, M1),
    ?assertEqual(M1, get_update_metadata(O2)),
    ?assertEqual("x-application/custom", get_update_content_type(O1)).

update_content_type_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    undefined = get_update_content_type(O),
    O1 = update_content_type(O, "application/json"),
    ?assertEqual("application/json", get_update_content_type(O1)).

get_update_data_test() ->
    MD0 = dict:from_list([{?MD_CTYPE, "text/plain"}]),
    MD1 = dict:from_list([{?MD_CTYPE, "application/json"}]),
    O = new_obj(<<"b">>, <<"k">>, <<"">>,
								[#r_content{metadata = MD0, value = <<"v">>}]),
    %% Create an updated metadata object
    Oumd = update_metadata(O, MD1),
    %% Create an updated value object
    Ouv = update_value(O, <<"valueonly">>),
    %% Create updated both object
    Oboth = update_value(Oumd, <<"both">>),

    %% dbgh:start(),
    %% dbgh:trace(?MODULE)
    % io:format("O=~p\n", [O]),
    ?assertEqual(<<"v">>, get_update_value(O)),
    MD2 = get_update_metadata(O),
    io:format("MD2=~p\n", [MD2]),
    ?assertEqual("text/plain", md_ctype(MD2)),

    MD3 = get_update_metadata(Oumd),
    ?assertEqual("application/json", md_ctype(MD3)),

    ?assertEqual(<<"valueonly">>, get_update_value(Ouv)),
    MD4 = get_update_metadata(Ouv),
    ?assertEqual("text/plain", md_ctype(MD4)),

    ?assertEqual(<<"both">>, get_update_value(Oboth)),
    MD5 = get_update_metadata(Oboth),
    ?assertEqual("application/json", md_ctype(MD5)).
   
%% get_update_data_sibs_test() ->
%%     MD0 = dict:from_list([{?MD_CTYPE, "text/plain"}]),
%%     MD1 = dict:from_list([{?MD_CTYPE, "application/json"}]),
%%     O = new_obj(<<"b">>, <<"k">>, <<"">>, 
%%                 [{MD0, <<"v">>},{MD1, <<"sibling">>}]),
%%     %% Create an updated metadata object
%%     Oumd = update_metadata(O, MD1),
%%     %% Create an updated value object
%%     Ouv = update_value(O, <<"valueonly">>),
%%     %% Create updated both object
%%     Oboth = update_value(Oumd, <<"both">>),
    
%%     ?assertThrow({error, siblings}, get_update_data(O)),
%%     ?assertThrow({error, siblings}, get_update_data(Oumd)),
%%     ?assertEqual({error, siblings}, get_update_data(Ouv)),
%%     ?assertEqual({ok, {MD1, <<"both">>}}, get_update_data(Oboth)).
                              
select_sibling_test() ->
    MD0 = dict:from_list([{?MD_CTYPE, "text/plain"}]),
    MD1 = dict:from_list([{?MD_CTYPE, "application/json"}]),
    O = new_obj(<<"b">>, <<"k">>, <<"">>,
                      [#r_content{metadata = MD0, value = <<"sib_one">>},
                       #r_content{metadata = MD1, value = <<"sib_two">>}]),
    O1 = select_sibling(1, O),
    O2 = select_sibling(2, O),
    ?assertEqual("text/plain", get_update_content_type(O1)),
    ?assertEqual(<<"sib_one">>, get_update_value(O1)),
    ?assertEqual("application/json", get_update_content_type(O2)),
    ?assertEqual(<<"sib_two">>, get_update_value(O2)).
   
-endif.
