defmodule Selecto.Fields do
  @moduledoc """
  Field access and resolution operations for Selecto.

  This module provides functions to access and resolve field information
  from a Selecto configuration, including custom columns, joins, filters,
  and field suggestions.
  """

  @doc """
  Get all filters defined in the domain configuration.

  ## Examples

      filters = Selecto.Fields.filters(selecto)
      # => %{"active" => %{name: "Active", type: "boolean", ...}}
  """
  @spec filters(Selecto.Types.t()) :: %{String.t() => term()}
  def filters(selecto) do
    selecto.config.filters
  end

  @doc """
  Get all columns available in the query.

  Returns a map of all columns from the source table and joined tables.

  ## Examples

      columns = Selecto.Fields.columns(selecto)
      # => %{id: %{name: "id", type: :integer, ...}, ...}
  """
  @spec columns(Selecto.Types.t()) :: map()
  def columns(selecto) do
    Map.get(selecto.config, :columns, %{})
  end

  @doc """
  Get all join configurations for the query.

  ## Examples

      joins = Selecto.Fields.joins(selecto)
      # => %{posts: %{type: :left, ...}, ...}
  """
  @spec joins(Selecto.Types.t()) :: map()
  def joins(selecto) do
    Map.get(selecto.config, :joins, %{})
  end

  @doc """
  Get the source table name.

  ## Examples

      table = Selecto.Fields.source_table(selecto)
      # => "users"
  """
  @spec source_table(Selecto.Types.t()) :: Selecto.Types.table_name() | nil
  def source_table(selecto) do
    Map.get(selecto.config, :source_table, nil)
  end

  @doc """
  Get the domain configuration.

  ## Examples

      domain = Selecto.Fields.domain(selecto)
      # => %{name: "User", source: %{...}, ...}
  """
  @spec domain(Selecto.Types.t()) :: Selecto.Types.domain()
  def domain(selecto) do
    selecto.domain
  end

  @doc """
  Get custom domain data.

  ## Examples

      data = Selecto.Fields.domain_data(selecto)
  """
  @spec domain_data(Selecto.Types.t()) :: term()
  def domain_data(selecto) do
    selecto.config.domain_data
  end

  @doc """
  Get the current query set (selected fields, filters, etc.).

  ## Examples

      query_set = Selecto.Fields.set(selecto)
      # => %{selected: ["id", "name"], filtered: [...], ...}
  """
  @spec set(Selecto.Types.t()) :: Selecto.Types.query_set()
  def set(selecto) do
    selecto.set
  end

  @doc """
  Get field information by field name.

  Supports custom columns, joined fields, and regular fields.
  Handles various field name formats and provides compatibility mappings.

  ## Examples

      field_info = Selecto.Fields.field(selecto, "customer[name]")
      # => %{name: "name", field: "name", requires_join: :customer, ...}
  """
  @spec field(Selecto.Types.t(), Selecto.Types.field_name()) :: map() | nil
  def field(selecto, field_name) do
    # First check custom columns (preserves ALL properties including group_by_filter)
    field_str = to_string(field_name)

    # Check both domain and config for custom columns
    # Try multiple possible field formats
    custom_col = get_in(selecto.domain, [:custom_columns, field_str]) ||
                 get_in(selecto.domain, [:custom_columns, to_string(field_name)]) ||
                 # Also try with underscores if field contains them
                 (if String.contains?(field_str, "_") do
                   # Try camelCase version
                   camel = field_str |> String.split("_") |> Enum.map(&String.capitalize/1) |> Enum.join("")
                   snake_case = String.downcase(camel, :ascii) |> String.replace(~r/([A-Z])/, "_\\1") |> String.trim_leading("_")
                   get_in(selecto.domain, [:custom_columns, camel]) ||
                   get_in(selecto.domain, [:custom_columns, snake_case])
                 end) ||
                 get_in(selecto.config, [:domain_data, :custom_columns, field_str]) ||
                 get_in(selecto.config, [:custom_columns, field_str])

    if custom_col do
      # Return the full custom column definition with all properties intact
      # Ensure requires_join is set (defaults to :selecto_root for root-level custom columns)
      custom_col
      |> Map.put_new(:requires_join, :selecto_root)
      |> Map.put_new(:colid, field_str)
    else
      # Try enhanced field resolution for regular fields
      case Selecto.FieldResolver.resolve_field(selecto, field_name) do
        {:ok, field_info} ->
          # Return complete field info with compatibility mappings
          field_info
          |> Map.put(:requires_join, field_info[:source_join])
          |> Map.put(:field, field_info[:field] || field_info[:name])

        {:error, _} ->
          # Fallback to config columns
          fallback_result = selecto.config.columns[field_name] || selecto.config.columns[String.to_atom(field_name)]

          if fallback_result do
            # Ensure the field property contains the database field name
            database_field = case Map.get(fallback_result, :field) do
              atom when is_atom(atom) -> Atom.to_string(atom)
              string when is_binary(string) -> string
              nil ->
                # Extract field name from colid if available, otherwise use the field parameter
                case Map.get(fallback_result, :colid) do
                  colid when is_binary(colid) ->
                    case Regex.run(~r/\[([^\]]+)\]$/, colid) do
                      [_, field_name] -> field_name
                      nil -> Atom.to_string(field_name)
                    end
                  _ -> Atom.to_string(field_name)
                end
            end
            Map.put(fallback_result, :field, database_field)
          else
            fallback_result
          end
      end
    end
  end

  @doc """
  Enhanced field resolution with disambiguation and error handling.

  Provides detailed field information and helpful error messages.

  ## Examples

      case Selecto.Fields.resolve_field(selecto, "name") do
        {:ok, field_info} -> # use field_info
        {:error, reason} -> # handle error
      end
  """
  @spec resolve_field(Selecto.Types.t(), Selecto.Types.field_name()) ::
    {:ok, map()} | {:error, term()}
  def resolve_field(selecto, field_name) do
    Selecto.FieldResolver.resolve_field(selecto, field_name)
  end

  @doc """
  Get all available fields across all joins and the source table.

  ## Examples

      fields = Selecto.Fields.available_fields(selecto)
      # => ["id", "name", "email", "posts[title]", ...]
  """
  @spec available_fields(Selecto.Types.t()) :: [String.t()]
  def available_fields(selecto) do
    Selecto.FieldResolver.get_available_fields(selecto)
  end

  @doc """
  Get field suggestions for autocomplete or error recovery.

  ## Examples

      suggestions = Selecto.Fields.field_suggestions(selecto, "cust")
      # => ["customer[id]", "customer[name]", "customer[email]"]
  """
  @spec field_suggestions(Selecto.Types.t(), String.t()) :: [String.t()]
  def field_suggestions(selecto, partial_name) do
    Selecto.FieldResolver.suggest_fields(selecto, partial_name)
  end
end