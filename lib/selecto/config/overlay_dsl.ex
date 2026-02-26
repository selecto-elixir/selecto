defmodule Selecto.Config.OverlayDSL do
  @moduledoc """
  A DSL (Domain-Specific Language) for defining overlay configurations.

  This module provides a clean, declarative syntax for customizing Selecto domains
  through overlay files. Instead of manually constructing maps, you can use
  macros like `defcolumn` and `deffilter` along with module attributes.

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

    jsonb_schemas_map =
      jsonb_schemas
      |> Enum.map(fn {name, fields} -> {name, fields} end)
      |> Map.new()

    extension_overlay = Selecto.Extensions.overlay_fragments(env.module, extension_specs)

    overlay =
      %{
        columns: columns_map,
        filters: filters_map,
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

  # Helper to extract configuration from block at compile time
  defp extract_config({:__block__, _, exprs}, _caller) do
    Enum.reduce(exprs, %{}, &process_directive/2)
  end

  defp extract_config(expr, _caller) do
    process_directive(expr, %{})
  end

  defp process_directive({directive, _, [value]}, acc) do
    Map.put(acc, directive, value)
  end

  defp process_directive(_, acc), do: acc

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
