%%%-------------------------------------------------------------------
%%% @copyright (C) 2014-2017, 2600Hz INC
%%% @doc
%%%
%%% Handle client requests for phone_number documents
%%%
%%% @end
%%% @contributors
%%%   James Aimonetti
%%%   Pierre Fenoll
%%%-------------------------------------------------------------------
-module(knm_vitelity).
-behaviour(knm_gen_carrier).

-export([is_local/0]).
-export([find_numbers/3]).
-export([acquire_number/1]).
-export([disconnect_number/1]).
-export([should_lookup_cnam/0]).
-export([is_number_billable/1]).
-export([check_numbers/1]).

-include("knm.hrl").
-include("knm_vitelity.hrl").


%%--------------------------------------------------------------------
%% @public
%% @doc
%% Is this carrier handling numbers local to the system?
%% Note: a non-local (foreign) carrier module makes HTTP requests.
%% @end
%%--------------------------------------------------------------------
-spec is_local() -> boolean().
is_local() -> 'false'.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Check with carrier if these numbers are registered with it.
%% @end
%%--------------------------------------------------------------------
-spec check_numbers(ne_binaries()) -> {ok, kz_json:object()} |
                                      {error, any()}.
check_numbers(_Numbers) -> {error, not_implemented}.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Query the Vitelity system for a quanity of available numbers
%% in a rate center
%% @end
%%--------------------------------------------------------------------
-spec find_numbers(ne_binary(), pos_integer(), knm_carriers:options()) ->
                          {'ok', knm_number:knm_numbers()} |
                          {'error', any()}.
find_numbers(Prefix, Quantity, Options) ->
    case props:is_true(tollfree, Options, 'false') of
        'false' -> classify_and_find(Prefix, Quantity, Options);
        'true' ->
            TFOpts = tollfree_options(Quantity, Options),
            find(Prefix, Quantity, Options, TFOpts)
    end.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Acquire a given number from the carrier
%% @end
%%--------------------------------------------------------------------
-spec acquire_number(knm_number:knm_number()) ->
                            knm_number:knm_number().
acquire_number(Number) ->
    PhoneNumber = knm_number:phone_number(Number),
    DID = knm_phone_number:number(PhoneNumber),
    case knm_converters:classify(DID) of
        <<"tollfree_us">> ->
            query_vitelity(Number, purchase_tollfree_options(DID));
        <<"tollfree">> ->
            query_vitelity(Number, purchase_tollfree_options(DID));
        _ ->
            query_vitelity(Number, purchase_local_options(DID))
    end.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Release a number from the routing table
%% @end
%%--------------------------------------------------------------------
-spec disconnect_number(knm_number:knm_number()) ->
                               knm_number:knm_number().
disconnect_number(Number) ->
    PhoneNumber = knm_number:phone_number(Number),
    DID = knm_phone_number:number(PhoneNumber),
    lager:debug("attempting to disconnect ~s", [DID]),
    query_vitelity(Number, release_did_options(DID)).

%%--------------------------------------------------------------------
%% @public
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec should_lookup_cnam() -> boolean().
should_lookup_cnam() -> 'false'.

%%--------------------------------------------------------------------
%% @public
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec is_number_billable(knm_phone_number:knm_phone_number()) -> boolean().
is_number_billable(_) -> 'true'.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec classify_and_find(ne_binary(), pos_integer(), knm_carriers:options()) ->
                               {'ok', knm_number:knm_numbers()} |
                               {'error', any()}.
classify_and_find(Prefix, Quantity, Options) ->
    case knm_converters:classify(Prefix) of
        <<"tollfree_us">> ->
            find(Prefix, Quantity, Options, tollfree_options(Quantity, Options));
        <<"tollfree">> ->
            find(Prefix, Quantity, Options, tollfree_options(Quantity, Options));
        _Classification ->
            find(Prefix, Quantity, Options, local_options(Prefix, Options))
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec tollfree_options(pos_integer(), knm_carriers:options()) ->
                              knm_vitelity_util:query_options().
tollfree_options(Quantity, Options) ->
    TollFreeOptions = [{'qs', [{'cmd', <<"listtollfree">>}
                              ,{'limit', Quantity}
                              ,{'xml', <<"yes">>}
                               | knm_vitelity_util:default_options(Options)
                              ]}
                      ,{'uri', knm_vitelity_util:api_uri()}
                      ],
    F = fun knm_vitelity_util:add_options_fold/2,
    lists:foldl(F, [], TollFreeOptions).

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec local_options(ne_binary(), knm_carriers:options()) ->
                           knm_vitelity_util:query_options().
local_options(Prefix, Options) when byte_size(Prefix) =< 3 ->
    LocalOptions = [{'qs', [{'npa', Prefix}
                           ,{'cmd', <<"listnpa">>}
                           ,{'withrates', knm_vitelity_util:get_query_value(<<"withrates">>, Options)}
                           ,{'type', knm_vitelity_util:get_query_value(<<"type">>, Options)}
                           ,{'provider', knm_vitelity_util:get_query_value(<<"provider">>, Options)}
                           ,{'xml', <<"yes">>}
                           ,{'cnam', knm_vitelity_util:get_query_value(<<"cnam">>, Options)}
                            | knm_vitelity_util:default_options(Options)
                           ]}
                   ,{'uri', knm_vitelity_util:api_uri()}
                   ],
    F = fun knm_vitelity_util:add_options_fold/2,
    lists:foldl(F, [], LocalOptions);

local_options(Prefix, Options) ->
    LocalOptions = [{'qs', [{'npanxx', Prefix}
                           ,{'cmd', <<"listnpanxx">>}
                           ,{'withrates', knm_vitelity_util:get_query_value(<<"withrates">>, Options)}
                           ,{'type', knm_vitelity_util:get_query_value(<<"type">>, Options)}
                           ,{'provider', knm_vitelity_util:get_query_value(<<"provider">>, Options)}
                           ,{'xml', <<"yes">>}
                           ,{'cnam', knm_vitelity_util:get_query_value(<<"cnam">>, Options)}
                            | knm_vitelity_util:default_options(Options)
                           ]}
                   ,{'uri', knm_vitelity_util:api_uri()}
                   ],
    F = fun knm_vitelity_util:add_options_fold/2,
    lists:foldl(F, [], LocalOptions).

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec find(ne_binary(), pos_integer(), knm_carriers:options(), knm_vitelity_util:query_options()) ->
                  {'ok', knm_number:knm_numbers()} |
                  {'error', any()}.
find(Prefix, Quantity, Options, VitelityOptions) ->
    case query_vitelity(Prefix, Quantity, VitelityOptions) of
        {'error', _}=Error -> Error;
        {'ok', JObj} -> response_to_numbers(JObj, Options)
    end.

response_to_numbers(JObj, Options) ->
    QID = knm_search:query_id(Options),
    Ns = [to_number(Num, CarrierData, QID)
          || {Num, CarrierData} <- kz_json:to_proplist(JObj)
         ],
    {'ok', Ns}.

to_number(DID, CarrierData, QID) ->
    {QID, {DID, ?MODULE, ?NUMBER_STATE_DISCOVERY, CarrierData}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec query_vitelity(ne_binary(), pos_integer(), knm_vitelity_util:query_options()) ->
                            {'ok', kz_json:object()} |
                            {'error', any()}.
-ifdef(TEST).
query_vitelity(Prefix, Quantity, QOptions) ->
    URI = knm_vitelity_util:build_uri(QOptions),
    {'ok'
    ,{'http', [], _Host, _Port, _Path, [$? | QueryString]}
    } = http_uri:parse(kz_util:to_list(URI)),
    Options = cow_qs:parse_qs(kz_util:to_binary(QueryString)),
    XML =
        case props:get_value(<<"cmd">>, Options) of
            ?PREFIX_SEARCH_CMD -> ?PREFIX_SEARCH_RESP;
            ?NUMBER_SEARCH_CMD -> ?NUMBER_SEARCH_RESP;
            ?TOLLFREE_SEARCH_CMD -> ?TOLLFREE_SEARCH_RESP
        end,
    process_xml_resp(Prefix, Quantity, XML).

-else.
query_vitelity(Prefix, Quantity, QOptions) ->
    URI = knm_vitelity_util:build_uri(QOptions),
    lager:debug("querying ~s", [URI]),
    case kz_http:post(kz_util:to_list(URI)) of
        {'ok', _RespCode, _RespHeaders, RespXML} ->
            lager:debug("recv ~p: ~s", [_RespCode, RespXML]),
            process_xml_resp(Prefix, Quantity, RespXML);
        {'error', _R} ->
            lager:debug("error querying: ~p", [_R]),
            {'error', 'not_available'}
    end.
-endif.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec process_xml_resp(ne_binary(), pos_integer(), text()) ->
                              {'ok', kz_json:object()} |
                              {'error', any()}.
process_xml_resp(Prefix, Quantity, XML) ->
    try xmerl_scan:string(XML) of
        {XmlEl, _} -> process_xml_content_tag(Prefix, Quantity, XmlEl)
    catch
        _E:_R ->
            ?LOG_DEBUG("failed to decode xml: ~s: ~p", [_E, _R]),
            {'error', 'xml_decode_failed'}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec process_xml_content_tag(ne_binary(), pos_integer(), xml_el()) ->
                                     {'ok', kz_json:object()} |
                                     {'error', any()}.
process_xml_content_tag(Prefix, Quantity, #xmlElement{name='content'
                                                     ,content=Children
                                                     }) ->
    Els = kz_xml:elements(Children),
    case knm_vitelity_util:xml_resp_status_msg(Els) of
        <<"fail">> ->
            {'error', knm_vitelity_util:xml_resp_error_msg(Els)};
        Status when Status =:= <<"ok">>;
                    Status =:= 'undefined' ->
            process_xml_numbers(Prefix, Quantity, knm_vitelity_util:xml_resp_numbers(Els))
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec process_xml_numbers(ne_binary(), pos_integer(), 'undefined' | xml_el()) ->
                                 {'ok', kz_json:object()} |
                                 {'error', any()}.
-spec process_xml_numbers(ne_binary(), pos_integer(), 'undefined' | xml_els(), kz_proplist()) ->
                                 {'ok', kz_json:object()} |
                                 {'error', any()}.
process_xml_numbers(_Prefix, _Quantity, 'undefined') ->
    {'error', 'no_numbers'};
process_xml_numbers(Prefix, Quantity, #xmlElement{name='numbers'
                                                 ,content=Content
                                                 }) ->
    process_xml_numbers(Prefix, Quantity, kz_xml:elements(Content), []).

process_xml_numbers(_Prefix, 0, _Els, Acc) ->
    {'ok', kz_json:from_list(Acc)};
process_xml_numbers(_Prefix, _Quantity, [#xmlElement{name='response'
                                                    ,content=Reason
                                                    }
                                         |_], _Acc) ->
    {'error', kz_xml:texts_to_binary(Reason)};
process_xml_numbers(_Prefix, _Quantity, [], Acc) ->
    {'ok', kz_json:from_list(Acc)};
process_xml_numbers(Prefix, Quantity, [El|Els], Acc) ->
    JObj = xml_did_to_json(El),

    case number_matches_prefix(JObj, Prefix) of
        'true' ->
            N = kz_json:get_value(<<"number">>, JObj),
            process_xml_numbers(Prefix, Quantity-1, Els, [{N, JObj}|Acc]);
        'false' ->
            process_xml_numbers(Prefix, Quantity, Els, Acc)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec number_matches_prefix(kz_json:object(), ne_binary()) -> boolean().
number_matches_prefix(JObj, Prefix) ->
    PrefixLen = byte_size(Prefix),
    CountryCode = kz_json:get_value(<<"country_code">>, JObj, <<"+1">>),
    CountryCodeLen = byte_size(CountryCode),
    case kz_json:get_value(<<"number">>, JObj) of
        <<Prefix:PrefixLen/binary, _/binary>> -> 'true';
        <<CountryCode:CountryCodeLen/binary, Prefix:PrefixLen/binary, _/binary>> -> 'true';
        _N -> 'false'
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec xml_did_to_json(xml_el()) -> kz_json:object().
xml_did_to_json(#xmlElement{name='did'
                           ,content=[#xmlText{}]=DID
                           }) ->
    kz_json:from_list([{<<"number">>
                       ,knm_converters:normalize(
                          kz_xml:texts_to_binary(DID)
                         )
                       }
                      ]);
xml_did_to_json(#xmlElement{name='did'
                           ,content=DIDInfo
                           }) ->
    kz_json:from_list(
      knm_vitelity_util:xml_els_to_proplist(
        kz_xml:elements(DIDInfo)
       ));
xml_did_to_json(#xmlElement{name='number'
                           ,content=DIDInfo
                           }) ->
    kz_json:from_list(
      knm_vitelity_util:xml_els_to_proplist(
        kz_xml:elements(DIDInfo)
       )).

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec purchase_local_options(ne_binary()) -> knm_vitelity_util:query_options().
purchase_local_options(DID) ->
    [{'qs', [{'did', knm_converters:to_npan(DID)}
            ,{'cmd', <<"getlocaldid">>}
            ,{'xml', <<"yes">>}
            ,{'routesip', get_routesip()}
             | knm_vitelity_util:default_options()
            ]}
    ,{'uri', knm_vitelity_util:api_uri()}
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec purchase_tollfree_options(ne_binary()) -> knm_vitelity_util:query_options().
purchase_tollfree_options(DID) ->
    [{'qs', [{'did', knm_converters:to_npan(DID)}
            ,{'cmd', <<"gettollfree">>}
            ,{'xml', <<"yes">>}
            ,{'routesip', get_routesip()}
             | knm_vitelity_util:default_options()
            ]}
    ,{'uri', knm_vitelity_util:api_uri()}
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec get_routesip() -> api_binary().
-ifdef(TEST).
get_routesip() -> <<"1.2.3.4">>.
-else.
get_routesip() ->
    case knm_vitelity_util:get_routesip() of
        [Route|_] -> Route;
        Route -> Route
    end.
-endif.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec query_vitelity(knm_number:knm_number(), knm_vitelity_util:query_options()) ->
                            knm_number:knm_number().
query_vitelity(Number, QOptions) ->
    URI = knm_vitelity_util:build_uri(QOptions),
    ?LOG_DEBUG("querying ~s", [URI]),
    case kz_http:post(kz_util:to_list(URI)) of
        {'ok', _RespCode, _RespHeaders, RespXML} ->
            ?LOG_DEBUG("recv ~p: ~s", [_RespCode, RespXML]),
            process_xml_resp(Number, RespXML);
        {'error', Error} ->
            ?LOG_DEBUG("error querying: ~p", [Error]),
            knm_errors:by_carrier(?MODULE, Error, Number)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec process_xml_resp(knm_number:knm_number(), text()) ->
                              knm_number:knm_number().
process_xml_resp(Number, XML) ->
    try xmerl_scan:string(XML) of
        {XmlEl, _} -> process_xml_content_tag(Number, XmlEl)
    catch
        _E:_R ->
            ?LOG_DEBUG("failed to decode xml: ~s: ~p", [_E, _R]),
            knm_errors:by_carrier(?MODULE, 'failed_decode_resp', Number)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec process_xml_content_tag(knm_number:knm_number(), xml_el()) ->
                                     knm_number:knm_number().
process_xml_content_tag(Number, #xmlElement{name='content'
                                           ,content=Children
                                           }) ->
    Els = kz_xml:elements(Children),
    case knm_vitelity_util:xml_resp_status_msg(Els) of
        <<"fail">> ->
            Msg = knm_vitelity_util:xml_resp_error_msg(Els),
            ?LOG_DEBUG("xml status is 'fail': ~s", [Msg]),
            knm_errors:by_carrier(?MODULE, Msg, Number);
        <<"ok">> ->
            ?LOG_DEBUG("successful provisioning"),
            Number
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec release_did_options(ne_binary()) -> knm_vitelity_util:query_options().
release_did_options(DID) ->
    [{'qs', [{'did', knm_converters:to_npan(DID)}
            ,{'cmd', <<"removedid">>}
            ,{'xml', <<"yes">>}
             | knm_vitelity_util:default_options()
            ]}
    ,{'uri', knm_vitelity_util:api_uri()}
    ].
