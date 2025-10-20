defmodule Selecto.Config.OverlayDSL do
  @moduledoc """
  A DSL (Domain-Specific Language) for defining overlay configurations.

  This module provides a clean, declarative syntax for customizing Selecto domains
  through overlay files. Instead of manually constructing maps, you can use
  macros like `defcolumn` and `deffilter` along with module attributes.

  ## Usage

      defmodule MyApp.SelectoDomains.Overlays.ProductDomainOverlay do
        use Selecto.Config.OverlayDSL

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

  defmacro __using__(_opts) do
    quote do
      import Selecto.Config.OverlayDSL
      Module.register_attribute(__MODULE__, :overlay_columns, accumulate: true)
      Module.register_attribute(__MODULE__, :overlay_filters, accumulate: true)
      Module.register_attribute(__MODULE__, :redactions, accumulate: false)

      @before_compile Selecto.Config.OverlayDSL
    end
  end

  defmacro __before_compile__(env) do
    columns = Module.get_attribute(env.module, :overlay_columns) |> Enum.reverse()
    filters = Module.get_attribute(env.module, :overlay_filters) |> Enum.reverse()
    redactions = Module.get_attribute(env.module, :redactions) || []

    columns_map =
      columns
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    filters_map =
      filters
      |> Enum.map(fn {name, props} -> {name, Map.new(props)} end)
      |> Map.new()

    quote do
      def overlay do
        %{
          columns: unquote(Macro.escape(columns_map)),
          filters: unquote(Macro.escape(filters_map)),
          redact_fields: unquote(redactions)
        }
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
end
