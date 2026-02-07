%%% Core Records

-record(database_state, {hash, name, tree, relations, timestamp}).

-record(relation, {
    hash,                  % Content hash (finite) or generator hash (infinite)
    name,                  % Relation name (atom)
    tree,                  % Merkle tree (mutable only), undefined for immutable/generator-based
    schema,                % #{attribute_name => relation_name}
    constraints,           % #relation_constraints{} - all constraint types (1OP, 2OP, 3OP)
    cardinality,           % {finite, N} | aleph_zero | continuum
    generator,             % Generator function (immutable only), undefined for tree-based
    membership_criteria,   % #{attribute_name => constraint:relational_constraint()} - domain membership test
    provenance,            % attribute_provenance() - maps attribute to source relation
    lineage                % lineage_op() - operation tree showing derivation
}).

-record(tuple, {hash, relation, attribute_map}).
-record(attribute, {hash, value}).

%% Specification record for creating immutable relations
-record(domain, {
    name,                  % Atom - relation name
    schema,                % #{attribute_name => relation_name}
    generator,             % {module, function} - tuple generator
    membership_criteria,   % #{attribute_name => constraint:relational_constraint()} - domain membership
    cardinality            % {finite, N} | aleph_zero | continuum
}).

%% Attribute constraint (1OP - 1st Order Property)
%% Constrains values of a specific attribute in a relation
-record(attribute_constraint, {
    name :: atom(),                % Stable identifier for lifecycle ops
    attribute :: atom(),              % Attribute name (e.g., salary)
    domain :: atom() | #relation{},   % Domain or relation this attribute draws from
    constraints :: [term()]           % Additional relational constraints beyond domain membership
}).

-record(tuple_constraint, {
    name :: atom(),                % Stable identifier for lifecycle ops
    constraints :: [term()]
}).

%% Relation constraints - holds all types of constraints for a relation
-record(relation_constraints, {
    attribute_constraints :: #{atom() => [#attribute_constraint{}]}, % 1OPs - per-attribute named constraints
    tuple_constraints :: [#tuple_constraint{}],                     % 2OPs - inter-attribute constraints
    multi_tuple_constraints :: [term()]                             % 3OPs - multi-tuple constraints (future)
}).

%%% Cardinality Types

-type cardinality() :: {finite, non_neg_integer()}   % Finite set with N elements
                     | aleph_zero                    % Countably infinite (ℵ₀)
                     | continuum.                    % Uncountably infinite (2^ℵ₀)

-type constraints() :: #{atom() => constraint:relational_constraint()}.

-type boundedness_constraints() :: #{atom() => constraints()}.

%%% Generator Types

-type generator_fun() :: fun((constraints()) -> generator_result()).

-type generator_result() ::
    done                                   % No more tuples
  | {value, map(), generator_fun()}        % Tuple and continuation
  | {error, term()}.                       % Error

-type generator_spec() ::
    {primitive, atom()}                    % Built-in: naturals, integers, rationals
  | {custom, generator_fun()}              % User-defined generator
  | {take, atom(), pos_integer()}          % Take N from relation
  | {derived, relational_op()}.            % Derived from relational operation

-type relational_op() ::
    {select, atom(), constraints()}
  | {project, atom(), [atom()]}
  | {join, atom(), atom(), join_spec()}.

-type join_spec() ::
    {equijoin, atom()}                     % Natural join on attribute
  | {theta, atom(), atom(), atom()}.       % Theta join: op(attr1, attr2)

%%% Provenance Types (for updatable views)

%% Relation-level provenance (tracks operation lineage)
-type provenance() :: undefined
                    | {base, atom()}       % Base relation
                    | {join, provenance(), provenance()}
                    | {select, provenance(), constraints()}
                    | {project, provenance(), [atom()]}
                    | {take, provenance(), pos_integer()}.

%% Attribute-level provenance (tracks data lineage for each attribute)
%% Maps attribute name to its source (relation, original_attribute_name)
-type attribute_provenance() :: #{atom() => {atom(), atom()}}.

%% Tuple-level lineage (tracks complete operational history)
%% Operation tree showing how the tuple was derived
-type lineage_op() ::
    {base, atom()}                                      % Base relation
  | {select, fun(), lineage_op()}                       % Filter operation
  | {project, [atom()], lineage_op()}                   % Projection
  | {join, atom(), lineage_op(), lineage_op()}          % Join on attribute
  | {theta_join, fun(), lineage_op(), lineage_op()}     % Join with predicate
  | {sort, fun(), lineage_op()}                         % Sort operation
  | {take, pos_integer(), lineage_op()}                 % Limit operation
  | {aggregate, atom(), atom(), lineage_op()}.          % Aggregation (future)
