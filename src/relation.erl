-module(relation).
-export([new_attribute/2, new_tuple/1, new_relation/1]).

%% Attribute: A pair of (Name, Value), eg. ("Name", "Augusto")
%% Tuple: A set of attributes, eg. (("Name", "Augusto"), ("Age", 20), ("Surname", "Rengel"))
%% Relation: A set of tuples organized through some selector
%% Example Relation: f(x: Nat) = x + 1
%% |{(0, 1), (1, 2), (2, 3), ...}| = |N|
%% RelationCustomer = [(Username, Text), (Password, Text)]
%% RelationProduct = [(Name, Text), (Price, Real)]
%% RelationOrder = [(OrderId, Natural), ((Username, Text), 
%%                                       (Password, Text), RelationCustomer), ((Name, Text), 
%%                                                                             (Price, Real), RelationProduct)]

new_attribute(Name, Value) ->
    #{name => Name, value => Value}.

new_tuple(Attributes) when is_list(Attributes) ->
    Fun = fun (#{name := Name, value := Value}, Acc) ->
		  maps:put(Name, Value, Acc)
	  end,
    lists:foldl(Fun, #{}, Attributes).

new_relation(Tuples) when is_list(Tuples) ->
    sets:from_list(Tuples, [{version, 2}]).

%% SoapName1 = relation:new_attribute(name, "ABC").
%% SoapPrice1 = relation:new_attribute(price, 1.0).
%% SoapName2 = relation:new_attribute(name, "XYZ").
%% SoapPrice2 = relation:new_attribute(price, 1.25).
%% BazookaName = relation:new_attribute(name, "Bazzum").
%% BazookaPrice = relation:new_attribute(price, 413.0).
%% SwordName = relation:new_attribute(name, "Sword").
%% SwordPrice = relation:new_attribute(price, 100.0).
%% relation:new_relation([relation:new_tuple([SoapName, SoapPrice]), relation:new_tuple([BazookaName, BazookaPrice]), relation:new_tuple([SwordName, SwordPrice])]).

%% Jules = relation:new_attribute(customer_name, "Jules").
%% Emily = relation:new_attribute(customer_name, "Emily").
%% Customers = relation:new_relation([relation:new_tuple([Jules]),
%% 				   relation:new_tuple([Emily])]).

%% operations:project(operations:select(operations:cartesian_product(relation:new_relation([relation:new_tuple([SoapName1, SoapPrice1]), relation:new_tuple([SoapName2, SoapPrice2]), relation:new_tuple([BazookaName, BazookaPrice]), relation:new_tuple([SwordName, SwordPrice])]), Customers), fun (Tuple) -> maps:get(price, Tuple) =< 1.0 end), [name, customer_name]).
