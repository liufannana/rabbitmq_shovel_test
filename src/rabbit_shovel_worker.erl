%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_worker).
-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

-record(state, {inbound_conn, inbound_ch,
                name, type, config, inbound_uri}).

%% [1] Counts down until we shut down in all modes
%% [2] Counts down until we stop publishing in on-confirm mode

start_link(Type, Name, Config) ->
    gen_server:start_link(?MODULE, [Type, Name, Config], []).

%%---------------------------
%% Gen Server Implementation
%%---------------------------

init([Type, Name, Config]) ->
    gen_server:cast(self(), init),
    {ok, Shovel} = parse(Type, Name, Config),
    {ok, #state{name = Name, type = Type, config = Shovel}}.

parse(static,  Name, Config) -> rabbit_shovel_config:parse(Name, Config);
parse(dynamic, Name, Config) -> rabbit_shovel_parameters:parse(Name, Config).

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(init, State = #state{config = Config}) ->
    random:seed(now()),
    #shovel{sources = Sources} = Config,
    {InboundConn, InboundChan, InboundURI} =
        make_conn_and_chan(Sources#endpoint.uris),

    %% Don't trap exits until we have established connections so that
    %% if we try to shut down while waiting for a connection to be
    %% established then we don't block
    process_flag(trap_exit, true),

    (Sources#endpoint.resource_declaration)(InboundConn, InboundChan),

    NoAck = Config#shovel.ack_mode =:= no_ack,
    case NoAck of
        false -> Prefetch = Config#shovel.prefetch_count,
                 #'basic.qos_ok'{} =
                     amqp_channel:call(
                       InboundChan, #'basic.qos'{prefetch_count = Prefetch});
        true  -> ok
    end,

    #'basic.consume_ok'{} =
        amqp_channel:subscribe(
          InboundChan, #'basic.consume'{queue  = Config#shovel.queue,
                                        no_ack = NoAck},
          self()),

    State1 =
        State#state{inbound_conn = InboundConn, inbound_ch = InboundChan,
                    inbound_uri = InboundURI},
    {noreply, State1}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag,
                              exchange = _Exchange, routing_key = _RoutingKey},
             _Msg = #amqp_msg{props = _Props = #'P_basic'{}, payload = Payload}},
            State = #state{inbound_ch = InboundChan, config = #shovel{ack_mode = on_confirm}}) ->
    %% confirm the message
    pushwork_worker:send_tokens(Payload),
    ok = amqp_channel:cast(
           InboundChan, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State};
handle_info({#'basic.deliver'{delivery_tag = _Tag,
                              exchange = _Exchange, routing_key = _RoutingKey},
             _Msg = #amqp_msg{props = _Props = #'P_basic'{}}},
            State = #state{inbound_ch = _InboundChan}) ->

    {noreply, State};

handle_info(#'basic.cancel'{}, State = #state{name = Name}) ->
    {stop, {shutdown, restart}, State};

handle_info({'EXIT', InboundConn, Reason},
            State = #state{inbound_conn = InboundConn}) ->
    {stop, {inbound_conn_died, Reason}, State}.

terminate(_Reason, #state{inbound_conn = undefined, inbound_ch = undefined,
                         name = _Name, type = _Type}) ->
    ok;
terminate(_Reason, State) ->
    catch amqp_connection:close(State#state.inbound_conn,
                                ?MAX_CONNECTION_CLOSE_TIMEOUT),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------
%% Helpers
%%---------------------------
make_conn_and_chan(URIs) ->
    Nth = random:uniform(length(URIs)),
    io:format("the nth is ~p~n", [Nth]),
    URI = lists:nth(Nth, URIs),
    {ok, AmqpParam} = amqp_uri:parse(URI),
    {ok, Conn} = amqp_connection:start(AmqpParam),
    link(Conn),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    {Conn, Chan, list_to_binary(amqp_uri:remove_credentials(URI))}.