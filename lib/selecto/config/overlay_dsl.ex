defmodule Selecto.Config.OverlayDSL do
  @moduledoc """
  A DSL (Domain-Specific Language) for defining overlay configurations.

  This module provides a clean, declarative syntax for customizing Selecto domains
  through overlay files. Instead of manually constructing maps, you can use
  macros like `defcolumn`, `deffilter`, `defdetail_action`, `defcte`,
  `defvalues`, `defsubquery`, `defjoin`, `defschema`, and `defsource_assoc`
  along with module attributes.

  ## Usage

      defmodule MyApp.SelectoDomains.Overlays.ProductDomainOverlay do
        use Selecto.Config.OverlayDSL,
          # Selecto.Extensions.PostGIS is provided by the :selecto_postgis package
          extensions: [Selecto.Extensions.PostGIS]

        # Module attributes for common configurations
        @redactions [:internal_notes, :cost_price]

        # Column customizations
        defcolumn :price do
          label "Product Price"
          format :currency
          aggregate_functions [:sum, :avg, :min, :max]
        end

        defcolumn :description do
          label "Product Description"
          max_length 100
        end

        # Custom filters
        deffilter "price_range" do
          name "Price Range"
          type :string
          description "Filter products by price range (e.g., '10-100')"
        end

        deffilter "in_stock" do
          name "In Stock"
          type :boolean
          description "Show only items currently in stock"
        end

        # Named query members
        defcte :active_products do
          query &__MODULE__.active_products_cte/1
          columns ["id", "name"]
          join [owner_key: :id, related_key: :id, fields: :infer]
        end

        defvalues :status_lookup do
          rows [["active", "Active"], ["archived", "Archived"]]
          columns ["status", "label"]
          as "status_lookup"
          join [owner_key: :status, related_key: :status]
        end

        defsubquery :high_value_orders do
          query &__MODULE__.high_value_orders_subquery/1
          type :inner
          on [%{left: "id", right: "customer_id"}]
        end
      end

  ## Available Directives

  ### Module Attributes

  - `@redactions` - List of field atoms to redact from queries

  ### JSONB Schema (with `defjsonb_schema`)

  Define structured schemas for JSONB columns to enable typed access, filtering, and display:

      defjsonb_schema :attributes do
        field :color, :string, label: "Color"
        field :weight, :decimal, label: "Weight (kg)", precision: 2
        field :organic, :boolean, label: "Organic"
        field :certifications, {:array, :string}, label: "Certifications"
        field :dimensions, :object do
          field :width, :decimal, label: "Width"
          field :height, :decimal, label: "Height"
        end
      end

  Supported types: `:string`, `:integer`, `:decimal`, `:boolean`, `:date`, `:datetime`,
  `{:array, type}`, `:object` (with nested fields)

  ### Column Directives (within `defcolumn`)

  - `label/1` - Human-readable column label
  - `format/1` - Display format (`:currency`, `:percentage`, `:number`, `:date`, etc.)
  - `aggregate_functions/1` - List of allowed aggregations (`:sum`, `:avg`, `:count`, `:min`, `:max`)
  - `precision/1` - Numeric precision for decimal types
  - `max_length/1` - Maximum string length for display
  - `sortable/1` - Whether column can be sorted (boolean)
  - `filterable/1` - Whether column can be filtered (boolean)

  ### Filter Directives (within `deffilter`)

  - `name/1` - Human-readable filter name
  - `type/1` - Filter type (`:string`, `:integer`, `:boolean`, `:date`, etc.)
  - `description/1` - Help text for the filter
  - `required/1` - Whether filter is required (boolean)
  - `default/1` - Default value for the filter
  - `options/1` - List of valid options for select-type filters

  ### Detail Action Macros

  - `defdetail_action id do ... end` - Define a detail-row action under `detail_actions`
  - `defpopup id do ... end` - Define a modal detail-row action under `detail_actions`

  ### Query Member Macros

  - `defcte id do ... end` - Define a named CTE preset under `query_members.ctes`
  - `defvalues id do ... end` - Define a named VALUES preset under `query_members.values`
  - `defsubquery id do ... end` - Define a named subquery-join preset under `query_members.subqueries`
  - `deflateral id do ... end` - Define a named LATERAL preset under `query_members.laterals`
  - `defunnest id do ... end` - Define a named UNNEST preset under `query_members.unnests`

  ### Domain Registry Macros

  - `defjoin id, config` - Define a top-level join entry under `joins`
  - `defschema id, config` - Define a top-level schema entry under `schemas`
  - `defschema_assoc schema_id, assoc_id, config` - Define a schema `associations` entry
  - `defsource_assoc id, config` - Define a root `source.associations` entry

  ### Query Member Directives

  - `query/1` - Query builder (`fn -> selecto end`, `fn selecto -> selecto end`, or capture)
  - `columns/1` - Declared columns for CTE/VALUES presets
  - `join/1` - Auto-join options used by named member helpers
  - `rows/1` - VALUES data rows (alias: `data/1`)
  - `source/1` / `lateral_source/1` - LATERAL source tuple/query
  - `array_field/1` - UNNEST source field/expression
  - `join_type/1` - LATERAL join type (`:left`, `:inner`, etc.)
  - `ordinality/1` - UNNEST ordinality alias
  - `as/1` - VALUES alias name
  - `join_id/1` - Subquery join id override
  - `on/1` - Subquery join conditions
  - `base_query/1`, `recursive_query/1` - Recursive CTE query functions
  - `dependencies/1` - CTE dependencies
  - `kind/1` - Subquery preset kind (currently `:join`)

  ## Examples

  ### Basic Column Customization

      defcolumn :price do
        label "Product Price"
        format :currency
        precision 2
        aggregate_functions [:sum, :avg]
      end

  ### Complex Filter

      deffilter "status" do
        name "Order Status"
        type :string
        description "Filter by order status"
        options ["pending", "shipped", "delivered", "cancelled"]
        default "pending"
      end

  ### Using Redactions

      @redactions [:password, :secret_key, :internal_notes]

  ### Computed Properties

      defcolumn :total_value do
        label "Total Value"
        format :currency
        aggregate_functions [:sum]
        computed true
      end
  """

  defmacro __using__(opts) do
    expanded_extensions =
      opts
      |> Keyword.get(:extensions, [])
      |> List.wrap()
      |> Enum.map(&expand_extension_spec(&1, __CALLER__))

    extension_specs =
      expanded_extensions
      |> Selecto.Extensions.normalize_specs()

    extension_imports =
      extension_specs
      |> Selecto.Extensions.overlay_dsl_modules()
      |> Enum.map(fn extension_module ->
        quote do
          import unquote(extension_module)
        end
      end)

    quote do
      import Selecto.Config.OverlayDSL
      unquote_splicing(extension_imports)

      Module.register_attribute(__MODULE__, :overlay_columns, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_filters, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_detail_actions, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_ctes, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_values, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_subqueries, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_laterals, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_unnests, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_joins, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_schemas, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_schema_associations, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_source_associations, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_jsonb_schemas, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_extension_specs, accumulate: false)
      Module.register_attribute(__MODULE__, :redactions, accumulate: false)
      @overlay_extension_specs unquote(Macro.escape(extension_specs))

      Selecto.Extensions.setup_overlay_extensions(__MODULE__, @overlay_extension_specs)

      @before_compile Selecto.Config.OverlayDSL
    end
  end

  defp expand_extension_spec({module_ast, extension_opts}, caller) when is_list(extension_opts) do
    {Macro.expand(module_ast, caller), extension_opts}
  end

  defp expand_extension_spec(%{module: module_ast, opts: extension_opts}, caller)
       when is_list(extension_opts) or is_map(extension_opts) do
    %{module: Macro.expand(module_ast, caller), opts: extension_opts}
  end

  defp expand_extension_spec(module_ast, caller), do: Macro.expand(module_ast, caller)

  defmacro __before_compile__(env) do
    columns = Module.get_attribute(env.module, :overlay_columns) |> Enum.reverse()
    filters = Module.get_attribute(env.module, :overlay_filters) |> Enum.reverse()
    detail_actions = Module.get_attribute(env.module, :overlay_detail_actions) |> Enum.reverse()
    ctes = Module.get_attribute(env.module, :overlay_ctes) |> Enum.reverse()
    values = Module.get_attribute(env.module, :overlay_values) |> Enum.reverse()
    subqueries = Module.get_attribute(env.module, :overlay_subqueries) |> Enum.reverse()
    laterals = Module.get_attribute(env.module, :overlay_laterals) |> Enum.reverse()
    unnests = Module.get_attribute(env.module, :overlay_unnests) |> Enum.reverse()
    joins = Module.get_attribute(env.module, :overlay_joins) |> Enum.reverse()
    schemas = Module.get_attribute(env.module, :overlay_schemas) |> Enum.reverse()

    schema_associations =
      Module.get_attribute(env.module, :overlay_schema_associations) |> Enum.reverse()

    source_associations =
      Module.get_attribute(env.module, :overlay_source_associations) |> Enum.reverse()

    jsonb_schemas = Module.get_attribute(env.module, :overlay_jsonb_schemas) |> Enum.reverse()
    redactions = Module.get_attribute(env.module, :redactions) || []
    extension_specs = Module.get_attribute(env.module, :overlay_extension_specs) || []

    columns_map =
      columns
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    filters_map =
      filters
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    detail_actions_map =
      detail_actions
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    ctes_map =
      ctes
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    values_map =
      values
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    subqueries_map =
      subqueries
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    laterals_map =
      laterals
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    unnests_map =
      unnests
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    joins_map =
      joins
      |> Enum.map(fn {name, props} -> {name, normalize_overlay_value(props)} end)
      |> Map.new()

    schemas_map =
      schemas
      |> Enum.map(fn {name, props} -> {name, normalize_overlay_value(props)} end)
      |> Map.new()
      |> merge_schema_associations(schema_associations)

    source_associations_map =
      source_associations
      |> Enum.map(fn {name, props} -> {name, normalize_overlay_value(props)} end)
      |> Map.new()

    jsonb_schemas_map =
      jsonb_schemas
      |> Enum.map(fn {name, fields} -> {name, fields} end)
      |> Map.new()

    extension_overlay = Selecto.Extensions.overlay_fragments(env.module, extension_specs)

    overlay =
      %{
        columns: columns_map,
        filters: filters_map,
        detail_actions: detail_actions_map,
        query_members: %{
          ctes: ctes_map,
          values: values_map,
          subqueries: subqueries_map,
          laterals: laterals_map,
          unnests: unnests_map
        },
        joins: joins_map,
        schemas: schemas_map,
        source: %{associations: source_associations_map},
        jsonb_schemas: jsonb_schemas_map,
        redact_fields: redactions
      }
      |> Selecto.Extensions.deep_merge(extension_overlay)

    quote do
      def overlay do
        unquote(Macro.escape(overlay))
      end
    end
  end

  @doc """
  Defines a column customization.

  ## Example

      defcolumn :price do
        label "Product Price"
        format :currency
        aggregate_functions [:sum, :avg]
      end
  """
  defmacro defcolumn(column_name, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_columns {unquote(column_name), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a custom filter.

  ## Example

      deffilter "price_range" do
        name "Price Range"
        type :string
        description "Filter by price range"
      end
  """
  defmacro deffilter(filter_name, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_filters {unquote(filter_name), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a detail-row action.

  ## Example

      defdetail_action :customer_profile do
        name("Customer Profile")
        description("Open the customer profile in a new tab")
        type(:external_link)
        required_fields([:customer_id])
        payload(%{url_template: "https://app.example.test/customers/{{customer_id}}"})
      end
  """
  defmacro defdetail_action(action_id, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_detail_actions {unquote(action_id), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a modal detail-row action.

  This is a convenience wrapper around `defdetail_action` that defaults `type`
  to `:modal`.
  """
  defmacro defpopup(action_id, do: block) do
    config =
      block
      |> extract_config(__CALLER__)
      |> Map.put_new(:type, :modal)

    quote do
      @overlay_detail_actions {unquote(action_id), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a named CTE preset for `Selecto.with_cte/2`.

  ## Example

      defcte :active_customers do
        query &__MODULE__.active_customers_cte/1
        columns ["id", "name"]
        join [owner_key: :id, related_key: :id, fields: :infer]
      end
  """
  defmacro defcte(cte_id, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_ctes {unquote(cte_id), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a named VALUES preset for `Selecto.with_values/2`.

  ## Example

      defvalues :status_lookup do
        rows [["active", "Active"], ["inactive", "Inactive"]]
        columns ["status", "label"]
        as "status_lookup"
        join [owner_key: :status, related_key: :status]
      end
  """
  defmacro defvalues(values_id, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_values {unquote(values_id), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a named subquery preset for `Selecto.with_subquery/2`.

  ## Example

      defsubquery :high_value_orders do
        query &__MODULE__.high_value_orders_subquery/1
        type :inner
        on [%{left: "id", right: "customer_id"}]
      end
  """
  defmacro defsubquery(subquery_id, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_subqueries {unquote(subquery_id), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a named LATERAL preset for `Selecto.with_lateral/2`.

  ## Example

      deflateral :tag_expansion do
        source {:unnest, "\"selecto_root\".\"tags\""}
        as "tag_expansion"
        join_type :inner
      end
  """
  defmacro deflateral(lateral_id, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_laterals {unquote(lateral_id), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a named UNNEST preset for `Selecto.with_unnest/2`.

  ## Example

      defunnest :product_tags do
        array_field "tags"
        as "tag"
        ordinality "tag_position"
      end
  """
  defmacro defunnest(unnest_id, do: block) do
    config = extract_config(block, __CALLER__)

    quote do
      @overlay_unnests {unquote(unnest_id), unquote(Macro.escape(config))}
    end
  end

  @doc """
  Defines a join configuration under the top-level `joins` registry.

  ## Example

      defjoin :initiative, %{
        type: :left,
        schema: MyApp.Initiative,
        owner_key: :initiative_id,
        related_key: :id
      }
  """
  defmacro defjoin(join_id, join_config) do
    quote do
      @overlay_joins {unquote(join_id), unquote(join_config)}
    end
  end

  @doc """
  Defines a schema configuration under the top-level `schemas` registry.

  ## Example

      defschema :initiative, %{
        source_table: "initiatives",
        columns: %{id: %{type: :integer}, name: %{type: :string}}
      }
  """
  defmacro defschema(schema_id, schema_config) do
    quote do
      @overlay_schemas {unquote(schema_id), unquote(schema_config)}
    end
  end

  @doc """
  Defines an association under a schema entry in `schemas`.

  ## Example

      defschema_assoc(:bundle_parent_load, :split_parent_load, %{
        queryable: :split_parent_load,
        field: :split_parent_load,
        owner_key: :split_parent_id,
        related_key: :id
      })
  """
  defmacro defschema_assoc(schema_id, association_id, association_config) do
    quote do
      @overlay_schema_associations {unquote(schema_id), unquote(association_id),
                                    unquote(association_config)}
    end
  end

  @doc """
  Defines a root source association under `source.associations`.

  ## Example

      defsource_assoc(:bundle_parent_load, %{
        queryable: :bundle_parent_load,
        field: :bundle_parent_load,
        owner_key: :bundle_parent_id,
        related_key: :id
      })
  """
  defmacro defsource_assoc(association_id, association_config) do
    quote do
      @overlay_source_associations {unquote(association_id), unquote(association_config)}
    end
  end

  @doc """
  Defines a JSONB schema for a JSONB column, enabling typed field access,
  filtering, and display of structured JSON data.

  ## Example

      defjsonb_schema :attributes do
        field :color, :string, label: "Color"
        field :weight, :decimal, label: "Weight (kg)", precision: 2
        field :organic, :boolean, label: "Organic"
        field :origin_country, :string, label: "Country of Origin"
        field :certifications, {:array, :string}, label: "Certifications"
        field :dimensions, :object do
          field :width, :decimal, label: "Width"
          field :height, :decimal, label: "Height"
          field :depth, :decimal, label: "Depth"
        end
      end

  ## Supported Field Types

  - `:string` - Text values
  - `:integer` - Whole numbers
  - `:decimal` - Decimal numbers (supports `precision` option)
  - `:boolean` - True/false values
  - `:date` - Date values (ISO 8601 format)
  - `:datetime` - DateTime values (ISO 8601 format)
  - `{:array, type}` - Arrays of the specified type
  - `:object` - Nested object (use nested `field` calls)

  ## Field Options

  - `label` - Human-readable label for display
  - `precision` - Decimal precision (for `:decimal` type)
  - `required` - Whether the field is required (default: false)
  - `default` - Default value for the field
  - `filterable` - Whether the field can be filtered (default: true)
  - `sortable` - Whether the field can be sorted (default: true)
  - `format` - Display format (`:currency`, `:percentage`, etc.)
  """
  defmacro defjsonb_schema(column_name, do: block) do
    fields = extract_jsonb_fields(block)

    quote do
      @overlay_jsonb_schemas {unquote(column_name), unquote(Macro.escape(fields))}
    end
  end

  # Extract JSONB field definitions from block
  defp extract_jsonb_fields({:__block__, _, exprs}) do
    Enum.flat_map(exprs, &parse_jsonb_field/1)
  end

  defp extract_jsonb_fields(expr) do
    parse_jsonb_field(expr)
  end

  # Parse a single field definition
  defp parse_jsonb_field({:field, _, [name, type | rest]}) do
    opts = extract_field_options(rest)
    [build_jsonb_field(name, type, opts)]
  end

  defp parse_jsonb_field(_), do: []

  # Extract options from field arguments
  defp extract_field_options([]), do: %{}

  defp extract_field_options([[do: nested_block]]) do
    # Nested object with fields
    nested_fields = extract_jsonb_fields(nested_block)
    %{fields: nested_fields}
  end

  defp extract_field_options([opts]) when is_list(opts) do
    Map.new(opts)
  end

  defp extract_field_options([opts, [do: nested_block]]) when is_list(opts) do
    nested_fields = extract_jsonb_fields(nested_block)
    opts |> Map.new() |> Map.put(:fields, nested_fields)
  end

  defp extract_field_options(_), do: %{}

  # Build the field specification map
  defp build_jsonb_field(name, type, opts) do
    base = %{
      name: name,
      type: normalize_type(type),
      label: Map.get(opts, :label, humanize_name(name)),
      filterable: Map.get(opts, :filterable, true),
      sortable: Map.get(opts, :sortable, true)
    }

    base
    |> maybe_add(:precision, opts)
    |> maybe_add(:required, opts)
    |> maybe_add(:default, opts)
    |> maybe_add(:format, opts)
    |> maybe_add(:fields, opts)
  end

  defp normalize_type({:array, inner_type}), do: {:array, inner_type}
  defp normalize_type(type) when is_atom(type), do: type
  defp normalize_type(type), do: type

  defp humanize_name(name) when is_atom(name) do
    name
    |> Atom.to_string()
    |> String.replace("_", " ")
    |> String.split()
    |> Enum.map(&String.capitalize/1)
    |> Enum.join(" ")
  end

  defp maybe_add(map, key, opts) do
    case Map.get(opts, key) do
      nil -> map
      value -> Map.put(map, key, value)
    end
  end

  defp normalize_overlay_value(value) when is_list(value) do
    if Keyword.keyword?(value), do: Map.new(value), else: value
  end

  defp normalize_overlay_value(value), do: value

  defp merge_schema_associations(schemas_map, schema_associations) do
    Enum.reduce(schema_associations, schemas_map, fn {schema_id, association_id,
                                                      association_config},
                                                     acc ->
      normalized_association_config = normalize_overlay_value(association_config)

      Map.update(
        acc,
        schema_id,
        %{associations: %{association_id => normalized_association_config}},
        fn schema_config ->
          schema_config = normalize_overlay_value(schema_config)
          associations = Map.get(schema_config, :associations, %{})

          Map.put(
            schema_config,
            :associations,
            Map.put(associations, association_id, normalized_association_config)
          )
        end
      )
    end)
  end

  # Helper to extract configuration from block at compile time
  defp extract_config({:__block__, _, exprs}, caller) do
    Enum.reduce(exprs, %{}, &process_directive(&1, &2, caller))
  end

  defp extract_config(expr, caller) do
    process_directive(expr, %{}, caller)
  end

  defp process_directive({directive, _, [value_ast]}, acc, caller) do
    value =
      try do
        {evaluated, _} = Code.eval_quoted(value_ast, [], caller)
        evaluated
      rescue
        _ -> value_ast
      end

    Map.put(acc, directive, value)
  end

  defp process_directive(_, acc, _caller), do: acc

  @doc """
  Sets the human-readable label for a column or filter.
  """
  defmacro label(_value) do
    # This is just a placeholder - actual processing happens in extract_config
    quote do: nil
  end

  @doc """
  Sets the display format for a column.

  Common formats: `:currency`, `:percentage`, `:number`, `:date`, `:datetime`, `:yes_no`
  """
  defmacro format(_value), do: quote(do: nil)

  @doc """
  Sets the allowed aggregate functions for a column.

  Options: `:sum`, `:avg`, `:count`, `:min`, `:max`
  """
  defmacro aggregate_functions(_value), do: quote(do: nil)

  @doc """
  Sets the numeric precision for decimal columns.
  """
  defmacro precision(_value), do: quote(do: nil)

  @doc """
  Sets the maximum display length for string columns.
  """
  defmacro max_length(_value), do: quote(do: nil)

  @doc """
  Sets whether the column is sortable.
  """
  defmacro sortable(_value), do: quote(do: nil)

  @doc """
  Sets whether the column is filterable.
  """
  defmacro filterable(_value), do: quote(do: nil)

  @doc """
  Marks a column as computed (not from database).
  """
  defmacro computed(_value), do: quote(do: nil)

  # Filter directive implementations

  @doc """
  Sets the human-readable name for a filter.
  """
  defmacro name(_value), do: quote(do: nil)

  @doc """
  Sets the filter type.

  Common types: `:string`, `:integer`, `:boolean`, `:date`, `:datetime`, `:decimal`
  """
  defmacro type(_value), do: quote(do: nil)

  @doc """
  Sets the filter description/help text.
  """
  defmacro description(_value), do: quote(do: nil)

  @doc """
  Sets whether the filter is required.
  """
  defmacro required(_value), do: quote(do: nil)

  @doc """
  Sets the default value for the filter.
  """
  defmacro default(_value), do: quote(do: nil)

  @doc """
  Sets the valid options for a select-type filter.
  """
  defmacro options(_value), do: quote(do: nil)

  @doc """
  Sets required fields for a detail-row action.
  """
  defmacro required_fields(_value), do: quote(do: nil)

  @doc """
  Sets a payload map for a detail-row action.
  """
  defmacro payload(_value), do: quote(do: nil)

  @doc """
  Sets a query builder function for named query members.
  """
  defmacro query(_value), do: quote(do: nil)

  @doc """
  Sets declared columns for CTE/VALUES query members.
  """
  defmacro columns(_value), do: quote(do: nil)

  @doc """
  Sets join options for query member auto-join behavior.
  """
  defmacro join(_value), do: quote(do: nil)

  @doc """
  Sets VALUES rows for a named `defvalues` member.
  """
  defmacro rows(_value), do: quote(do: nil)

  @doc """
  Alias for `rows/1` in named VALUES members.
  """
  defmacro data(_value), do: quote(do: nil)

  @doc """
  Sets the SQL alias/table name for named VALUES members.
  """
  defmacro as(_value), do: quote(do: nil)

  @doc """
  Sets explicit join id for named subquery members.
  """
  defmacro join_id(_value), do: quote(do: nil)

  @doc """
  Sets ON conditions for named subquery members.
  """
  defmacro on(_value), do: quote(do: nil)

  @doc """
  Sets recursive CTE base query function.
  """
  defmacro base_query(_value), do: quote(do: nil)

  @doc """
  Sets recursive CTE recursive query function.
  """
  defmacro recursive_query(_value), do: quote(do: nil)

  @doc """
  Sets CTE dependency names.
  """
  defmacro dependencies(_value), do: quote(do: nil)

  @doc """
  Sets named subquery kind (`:join` currently supported).
  """
  defmacro kind(_value), do: quote(do: nil)

  @doc """
  Sets a source expression for named LATERAL members.
  """
  defmacro source(_value), do: quote(do: nil)

  @doc """
  Alias for `source/1` in named LATERAL members.
  """
  defmacro lateral_source(_value), do: quote(do: nil)

  @doc """
  Sets the UNNEST array field/expression for named UNNEST members.
  """
  defmacro array_field(_value), do: quote(do: nil)

  @doc """
  Sets the join type for named LATERAL members.
  """
  defmacro join_type(_value), do: quote(do: nil)

  @doc """
  Sets ordinality alias for named UNNEST members.
  """
  defmacro ordinality(_value), do: quote(do: nil)

  # JSONB Schema Field Directives

  @doc """
  Defines a field within a JSONB schema.

  This macro is only valid inside a `defjsonb_schema` block.

  ## Examples

      # Simple field with type and label
      field :color, :string, label: "Color"

      # Field with multiple options
      field :weight, :decimal, label: "Weight", precision: 2, required: true

      # Array field
      field :tags, {:array, :string}, label: "Tags"

      # Nested object field
      field :dimensions, :object do
        field :width, :decimal
        field :height, :decimal
      end
  """
  defmacro field(_name, _type, _opts \\ []) do
    # This is a placeholder - actual processing happens in extract_jsonb_fields
    quote do: nil
  end
end
