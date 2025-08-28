defmodule Selecto.Types do
  @moduledoc """
  Comprehensive type definitions for the Selecto query builder.
  
  This module defines all the types used throughout Selecto for better
  developer experience and Dialyzer support.
  """

  # Core primitive types
  @type atom_or_string :: atom() | String.t()
  @type field_name :: atom() | String.t()
  @type table_name :: String.t()

  # Database types
  @type column_type :: 
    :integer | :string | :text | :boolean | :decimal | :float |
    :date | :time | :utc_datetime | :naive_datetime | :binary |
    :uuid | :json | :jsonb | :array

  @type column_definition :: %{
    required(:type) => column_type(),
    optional(:precision) => pos_integer(),
    optional(:scale) => non_neg_integer(),
    optional(:default) => term(),
    optional(:null) => boolean()
  }

  # Join types
  @type basic_join_type :: :left | :right | :inner | :full

  @type advanced_join_type :: 
    :hierarchical | :tagging | :dimension | 
    :star_dimension | :snowflake_dimension

  @type join_type :: basic_join_type() | advanced_join_type()

  @type hierarchy_type :: :adjacency_list | :materialized_path | :closure_table

  # Selector types for SELECT clauses
  @type basic_selector :: field_name()
  
  @type function_selector :: {
    :func, 
    function_name :: String.t(),
    args :: [field_name() | term()]
  }

  @type case_selector :: {
    :case,
    conditions :: [{condition :: term(), value :: term()}],
    else_value :: term()
  }

  @type extract_selector :: {
    :extract,
    part :: String.t(),
    from_field :: field_name()
  }

  @type window_selector :: {
    :window,
    function_name :: String.t(),
    args :: [term()],
    window_spec :: term()
  }

  @type custom_selector :: {
    :custom,
    sql :: String.t(),
    params :: [term()]
  }

  @type selector :: 
    basic_selector() |
    function_selector() |
    case_selector() |
    extract_selector() |
    window_selector() |
    custom_selector()

  # Pivot feature types
  @type pivot_config :: %{
    required(:target_schema) => atom(),
    required(:join_path) => [atom()],
    optional(:preserve_filters) => boolean(),
    optional(:subquery_strategy) => :exists | :in | :join
  }

  @type pivot_join_path :: [%{
    required(:from_schema) => atom(),
    required(:to_schema) => atom(),
    required(:association_name) => atom(),
    required(:join_type) => join_type()
  }]

  # Subselect feature types
  @type subselect_format :: :json_agg | :array_agg | :string_agg | :count
  
  @type subselect_selector :: %{
    required(:fields) => [field_name()],
    required(:target_schema) => atom(),
    required(:format) => subselect_format(),
    optional(:alias) => String.t(),
    optional(:separator) => String.t(),
    optional(:order_by) => [order_spec()],
    optional(:filters) => [filter()]
  }

  @type subselect_config :: %{
    required(:target_table) => table_name(),
    required(:join_condition) => {field_name(), field_name()},
    required(:aggregation_type) => subselect_format(),
    optional(:additional_filters) => [filter()]
  }

  # Filter types for WHERE clauses
  @type comparison_operator :: 
    :eq | :not_eq | :gt | :gte | :lt | :lte |
    :like | :ilike | :not_like | :not_ilike |
    :is_null | :not_null | :in | :not_in |
    :between | :not_between

  @type basic_filter :: {field_name(), term()}
  @type comparison_filter :: {field_name(), {comparison_operator(), term()}}
  @type logical_filter :: {:and | :or, [filter()]}

  @type filter :: basic_filter() | comparison_filter() | logical_filter()

  # Order by types
  @type order_direction :: :asc | :desc
  @type order_spec :: field_name() | {order_direction(), field_name()}

  # Association types
  @type association :: %{
    required(:queryable) => atom(),
    required(:field) => atom(),
    required(:owner_key) => atom(),
    required(:related_key) => atom(),
    optional(:cardinality) => :one | :many,
    optional(:through) => [atom()]
  }

  # Join configuration types
  @type basic_join_config :: %{
    required(:type) => basic_join_type(),
    optional(:name) => String.t(),
    optional(:on) => term(),
    optional(:joins) => %{atom() => join_config()}
  }

  @type hierarchical_join_config :: %{
    required(:type) => :hierarchical,
    required(:hierarchy_type) => hierarchy_type(),
    optional(:depth_limit) => pos_integer(),
    optional(:path_field) => atom(),
    optional(:path_separator) => String.t(),
    optional(:root_condition) => term(),
    optional(:joins) => %{atom() => join_config()}
  }

  @type tagging_join_config :: %{
    required(:type) => :tagging,
    required(:tag_field) => atom(),
    optional(:weight_field) => atom(),
    optional(:min_weight) => number(),
    optional(:aggregation) => :string_agg | :array_agg | :count,
    optional(:separator) => String.t(),
    optional(:joins) => %{atom() => join_config()}
  }

  @type dimension_join_config :: %{
    required(:type) => :dimension | :star_dimension | :snowflake_dimension,
    required(:display_field) => atom(),
    optional(:dimension_key) => atom(),
    optional(:normalization_joins) => [%{required(:table) => String.t(), required(:key) => atom(), required(:foreign_key) => atom()}],
    optional(:joins) => %{atom() => join_config()}
  }

  @type join_config :: 
    basic_join_config() |
    hierarchical_join_config() |
    tagging_join_config() |
    dimension_join_config()

  # Schema types
  @type schema :: %{
    required(:name) => String.t(),
    required(:source_table) => table_name(),
    required(:primary_key) => atom(),
    required(:fields) => [atom()],
    required(:redact_fields) => [atom()],
    required(:columns) => %{atom() => column_definition()},
    required(:associations) => %{atom() => association()},
    optional(:custom_filters) => %{atom() => term()}
  }

  # Source (main table) configuration
  @type source :: %{
    required(:source_table) => table_name(),
    required(:primary_key) => atom(),
    required(:fields) => [atom()],
    required(:redact_fields) => [atom()],
    required(:columns) => %{atom() => column_definition()},
    required(:associations) => %{atom() => association()}
  }

  # Domain configuration
  @type domain :: %{
    required(:name) => String.t(),
    required(:source) => source(),
    required(:schemas) => %{atom() => schema()},
    required(:joins) => %{atom() => join_config()},
    optional(:default_selected) => [selector()],
    optional(:required_filters) => [filter()],
    optional(:required_selected) => [selector()],
    optional(:required_order_by) => [order_spec()],
    optional(:required_group_by) => [field_name()],
    optional(:filters) => %{String.t() => term()},
    optional(:domain_data) => term()
  }

  # Query set (mutable query state)
  @type query_set :: %{
    required(:selected) => [selector()],
    required(:filtered) => [filter()],
    required(:order_by) => [order_spec()],
    required(:group_by) => [field_name()],
    optional(:pivot_state) => pivot_config(),
    optional(:subselected) => [subselect_selector()]
  }

  # Main Selecto struct
  @type t :: %Selecto{
    postgrex_opts: Postgrex.conn(),
    domain: domain(),
    config: processed_config(),
    set: query_set()
  }

  # Processed configuration (internal)
  @type processed_config :: %{
    required(:source) => source(),
    required(:source_table) => table_name(),
    required(:primary_key) => atom(),
    required(:columns) => %{String.t() => %{required(:name) => String.t()}},
    required(:joins) => %{atom() => processed_join()},
    required(:filters) => %{String.t() => term()},
    required(:domain_data) => term()
  }

  @type processed_join :: %{
    required(:type) => join_type(),
    required(:source) => atom() | String.t(),
    required(:name) => String.t(),
    optional(:fields) => %{String.t() => %{required(:name) => String.t()}},
    optional(:filters) => %{String.t() => term()},
    optional(:joins) => %{atom() => processed_join()},
    optional(:parameters) => [term()],
    # Join-specific fields
    optional(:hierarchy_type) => hierarchy_type(),
    optional(:depth_limit) => pos_integer(),
    optional(:path_field) => atom(),
    optional(:path_separator) => String.t(),
    optional(:tag_field) => atom(),
    optional(:weight_field) => atom(),
    optional(:display_field) => atom(),
    optional(:dimension_key) => atom()
  }

  # SQL building types
  @type iodata_fragment :: iolist() | String.t()
  @type param_marker :: {:param, term()}
  @type cte_marker :: {:cte, name :: String.t(), iodata_fragment()}
  @type iodata_with_markers :: [iodata_fragment() | param_marker() | cte_marker()]

  @type sql_params :: [term()]
  @type sql_result :: {sql :: String.t(), params :: sql_params()}

  # CTE types
  @type cte_definition :: %{
    required(:name) => String.t(),
    required(:sql) => iodata_with_markers(),
    required(:params) => sql_params(),
    required(:recursive) => boolean()
  }

  # Query execution results
  @type query_result :: {
    rows :: [[term()]],
    columns :: [String.t()],
    aliases :: %{String.t() => String.t()}
  }

  
  # Safe execute/2 results (tagged tuples with structured errors)
  @type execute_result_ok :: {:ok, query_result()}
  @type execute_result_error :: {:error, Selecto.Error.t()}
  @type safe_execute_result :: execute_result_ok() | execute_result_error()
  
  # Single row execution results
  @type single_row_result :: {row :: [term()], aliases :: %{String.t() => String.t()}}
  @type execute_one_result_ok :: {:ok, single_row_result()}
  @type execute_one_result_error :: {:error, Selecto.Error.t()}  
  @type safe_execute_one_result :: execute_one_result_ok() | execute_one_result_error()

  # Builder types
  @type join_requirement :: {join_name :: atom(), required_for :: String.t()}
  @type join_dependencies :: [join_requirement()]

  @type builder_result :: {
    iodata_with_markers(),
    sql_params(),
    join_dependencies()
  }

  @type cte_builder_result :: {
    iodata_with_markers(),
    sql_params(), 
    [cte_definition()]
  }

  # Configuration options
  @type configure_options :: [
    validate: boolean()
  ]

  @type execute_options :: [
    timeout: timeout(),
    log: boolean()
  ]

  @type sql_generation_options :: [
    include_comments: boolean(),
    pretty_print: boolean()
  ]

  # Option provider types for select filters
  @type static_option_provider :: %{
    type: :static,
    values: [term()]
  }

  @type domain_option_provider :: %{
    type: :domain,
    domain: atom(),
    value_field: atom(),
    display_field: atom(),
    filters: [filter()],
    order_by: [order_spec()]
  }

  @type enum_option_provider :: %{
    type: :enum,
    schema: module(),
    field: atom()
  }

  @type query_option_provider :: %{
    type: :query,
    query: String.t(),
    params: [term()]
  }

  @type option_provider :: 
    static_option_provider() |
    domain_option_provider() |
    enum_option_provider() |
    query_option_provider()

  @type select_options_column :: %{
    required(:type) => :select_options,
    required(:option_provider) => option_provider(),
    required(:name) => String.t(),
    optional(:multiple) => boolean(),
    optional(:searchable) => boolean(),
    optional(:cache_ttl) => pos_integer()
  }

  # Error types
  @type sql_error :: Postgrex.Error.t()

  # Utility types for better error messages
  @type maybe(t) :: {:ok, t} | {:error, term()}
  @type result(t, e) :: {:ok, t} | {:error, e}
end