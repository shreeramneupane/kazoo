%%%-------------------------------------------------------------------
%%% @copyright (C) 2017, 2600Hz
%%% @author Karl Anderson <karl@2600hz.org>
%%% @doc
%%%
%%% @end
%%% Created : 22 Sep 2011 by Karl Anderson <karl@2600hz.org>
%%%-------------------------------------------------------------------
-module(braintree_discount).

-export([xml_to_record/1, xml_to_record/2]).
-export([record_to_xml/1, record_to_xml/2]).
-export([record_to_json/1]).
-export([json_to_record/1]).

-include("bt.hrl").

-export_type([discount/0
             ,discounts/0
             ]).

-type discount() :: bt_discount().
-type discounts() :: bt_discounts().

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Contert the given XML to a customer record
%% @end
%%--------------------------------------------------------------------
-spec xml_to_record(bt_xml()) -> bt_discount().
-spec xml_to_record(bt_xml(), kz_deeplist()) -> bt_discount().

xml_to_record(Xml) ->
    xml_to_record(Xml, "/discount").

xml_to_record(Xml, Base) ->
    #bt_discount{id = kz_util:get_xml_value([Base, "/id/text()"], Xml)
                ,amount = kz_util:get_xml_value([Base, "/amount/text()"], Xml)
                ,never_expires = kz_util:is_true(kz_util:get_xml_value([Base, "/never-expires/text()"], Xml))
                ,billing_cycle = kz_util:get_xml_value([Base, "/current-billing-cycle/text()"], Xml)
                ,number_of_cycles = kz_util:get_xml_value([Base, "/number-of-billing-cycles/text()"], Xml)
                ,quantity = kz_util:to_integer(kz_util:get_xml_value([Base, "/quantity/text()"], Xml))
                }.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Contert the given XML to a customer record
%% @end
%%--------------------------------------------------------------------
-spec record_to_xml(bt_discount()) -> proplist() | bt_xml().
-spec record_to_xml(bt_discount(), boolean()) -> proplist() | bt_xml().

record_to_xml(Discount) ->
    record_to_xml(Discount, false).

record_to_xml(Discount, ToString) ->
    Props = [{'id', Discount#bt_discount.id}
            ,{'amount', Discount#bt_discount.amount}
            ,{'never-expires', Discount#bt_discount.never_expires}
            ,{'number-of-billing-cycles', Discount#bt_discount.number_of_cycles}
            ,{'quantity', Discount#bt_discount.quantity}
            ,{'inherited-from-id', Discount#bt_discount.inherited_from}
            ,{'existing-id', Discount#bt_discount.id}
            ],
    case ToString of
        true -> braintree_util:make_doc_xml(Props, 'discount');
        false -> Props
    end.

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Convert a given record into a json object
%% @end
%%--------------------------------------------------------------------
-spec record_to_json(bt_discount()) -> kz_json:object().
record_to_json(#bt_discount{id=Id, amount=Amount, quantity=Q}) ->
    Props = [{<<"id">>, Id}
            ,{<<"amount">>, Amount}
            ,{<<"quantity">>, kz_util:to_integer(Q)}
            ],
    kz_json:from_list([KV || {_, V}=KV <- Props, V =/= 'undefined']).

%%--------------------------------------------------------------------
%% @public
%% @doc
%% Convert a given json obj into a record
%% @end
%%--------------------------------------------------------------------
-spec json_to_record(api_object()) -> bt_discount() | 'undefined'.
json_to_record('undefined') -> 'undefined';
json_to_record(JObj) ->
    #bt_discount{id = kz_doc:id(JObj)
                ,amount = kz_json:get_binary_value(<<"amount">>, JObj)
                ,never_expires = kz_json:get_value(<<"never_expires">>, JObj, 'true')
                ,billing_cycle = kz_json:get_binary_value(<<"billing_cycle">>, JObj)
                ,number_of_cycles = kz_json:get_binary_value(<<"number_of_cycles">>, JObj)
                ,quantity = kz_json:get_integer_value(<<"quantity">>, JObj)
                ,inherited_from = kz_json:get_binary_value(<<"inherited_from">>, JObj)
                ,existing_id = kz_json:get_binary_value(<<"existing_id">>, JObj)
                }.
