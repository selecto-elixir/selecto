defmodule Selecto.EctoAdapter do
  @moduledoc """
  Ecto integration for Selecto query builder.
  
  This module provides functionality to automatically configure Selecto
  from Ecto schemas and repositories, making it easy to integrate with
  Phoenix applications.
  
  ## Usage
  
      # Configure from Ecto repo and schema
      selecto = Selecto.EctoAdapter.configure(MyApp.Repo, MyApp.User)
      
      # With options
      selecto = Selecto.EctoAdapter.configure(MyApp.Repo, MyApp.User, 
        joins: [:posts, :comments],
        redact_fields: [:password_hash]
      )
      
      # Generate domain from schema
      domain = Selecto.EctoAdapter.schema_to_domain(MyApp.User)
  """

  @doc """
  Configure Selecto from an Ecto repository and schema.
  
  ## Parameters
  
  - `repo` - The Ecto repository module (e.g., MyApp.Repo)
  - `schema` - The Ecto schema module to use as the source table
  - `opts` - Configuration options
  
  ## Options
  
  - `:joins` - List of associations to include as joins (atoms)
  - `:redact_fields` - List of fields to exclude from queries (atoms) 
  - `:custom_columns` - Map of custom column definitions
  - `:custom_filters` - Map of custom filter definitions
  - `:validate` - Whether to validate domain configuration (boolean)
  - `:name` - Custom name for the domain (string)
  
  ## Examples
  
      # Basic usage
      selecto = Selecto.EctoAdapter.configure(MyApp.Repo, MyApp.User)
      
      # With joins and redacted fields
      selecto = Selecto.EctoAdapter.configure(MyApp.Repo, MyApp.User,
        joins: [:posts, :profile],
        redact_fields: [:password_hash, :email]
      )
      
      # With custom columns
      selecto = Selecto.EctoAdapter.configure(MyApp.Repo, MyApp.User,
        custom_columns: %{
          "full_name" => %{
            name: "Full Name",
            select: {:concat, ["first_name", {:literal, " "}, "last_name"]}
          }
        }
      )
  """
  def configure(repo, schema, opts \\ []) do
    domain = schema_to_domain(schema, opts)
    db_conn = get_db_connection(repo)
    
    Selecto.configure(domain, db_conn, Keyword.take(opts, [:validate]))
  end

  @doc """
  Generate a Selecto domain configuration from an Ecto schema.
  
  This function introspects the Ecto schema and generates a compatible
  domain map for Selecto configuration.
  
  ## Parameters
  
  - `schema` - The Ecto schema module
  - `opts` - Configuration options (see `configure/3`)
  
  ## Returns
  
  A domain map compatible with `Selecto.configure/2`
  """
  def schema_to_domain(schema, opts \\ []) do
    schema_info = introspect_schema(schema)
    joins_config = Keyword.get(opts, :joins, [])
    redact_fields = Keyword.get(opts, :redact_fields, [])
    custom_columns = Keyword.get(opts, :custom_columns, %{})
    custom_filters = Keyword.get(opts, :custom_filters, %{})
    domain_name = Keyword.get(opts, :name, schema_info.name)
    
    # Build the main source configuration
    source_config = %{
      source_table: schema_info.table,
      primary_key: schema_info.primary_key,
      fields: schema_info.fields -- redact_fields,
      redact_fields: redact_fields,
      columns: schema_info.columns,
      associations: build_associations(schema_info.associations, joins_config)
    }
    
    # Build schemas for joins
    schemas_config = build_join_schemas(schema_info.associations, joins_config)
    
    # Build join configuration
    joins_definition = build_joins_definition(schema_info.associations, joins_config)
    
    %{
      source: source_config,
      schemas: schemas_config,
      name: domain_name,
      custom_columns: custom_columns,
      filters: custom_filters,
      joins: joins_definition
    }
  end

  @doc """
  Get available associations from an Ecto schema.
  
  Returns a list of association names that can be used in joins.
  """
  def get_associations(schema) do
    schema_info = introspect_schema(schema)
    Map.keys(schema_info.associations)
  end

  @doc """
  Get field information from an Ecto schema.
  
  Returns a map with field names and their types.
  """
  def get_fields(schema) do
    schema_info = introspect_schema(schema)
    schema_info.columns
  end

  ## Private functions

  defp introspect_schema(schema) do
    # Get schema metadata
    source = schema.__schema__(:source)
    primary_key = List.first(schema.__schema__(:primary_key))
    fields = schema.__schema__(:fields)
    associations = schema.__schema__(:associations)
    
    # Build column type information
    columns = build_columns_map(schema, fields)
    
    # Build associations information
    assoc_info = build_associations_info(schema, associations)
    
    %{
      name: get_schema_name(schema),
      table: source,
      primary_key: primary_key,
      fields: fields,
      columns: columns,
      associations: assoc_info
    }
  end

  defp build_columns_map(schema, fields) do
    Enum.into(fields, %{}, fn field ->
      ecto_type = schema.__schema__(:type, field)
      selecto_type = ecto_type_to_selecto_type(ecto_type)
      {field, %{type: selecto_type}}
    end)
  end

  defp ecto_type_to_selecto_type(type) do
    case type do
      :id -> :integer
      :integer -> :integer
      :string -> :string
      :binary -> :string
      :boolean -> :boolean
      :decimal -> :decimal
      :float -> :float
      :date -> :date
      :time -> :time
      :utc_datetime -> :utc_datetime
      :naive_datetime -> :naive_datetime
      {:array, inner_type} -> {:array, ecto_type_to_selecto_type(inner_type)}
      {Ecto.Enum, _} -> :string
      _ -> :string  # Default fallback
    end
  end

  defp build_associations_info(schema, associations) do
    Enum.into(associations, %{}, fn assoc_name ->
      assoc = schema.__schema__(:association, assoc_name)
      
      assoc_info = %{
        queryable: get_association_schema(assoc),
        field: assoc_name,
        owner_key: get_association_owner_key(assoc),
        related_key: get_association_related_key(assoc),
        type: get_association_type(assoc)
      }
      
      {assoc_name, assoc_info}
    end)
  end

  defp get_association_schema(%{related: related}), do: related
  defp get_association_schema(%{through: [through, _]}), do: through

  defp get_association_owner_key(%{owner_key: owner_key}), do: owner_key
  defp get_association_owner_key(%{through: [_through_assoc, _]}) do
    # For has_through, the owner_key comes from the first association in the path
    # We'll use a default for now, but this might need schema introspection
    :id
  end

  defp get_association_related_key(%{related_key: related_key}), do: related_key
  defp get_association_related_key(%{through: [_, _related_assoc]}) do
    # For has_through, the related_key comes from the second association in the path
    # We'll use a default for now, but this might need schema introspection
    :id
  end

  defp get_association_type(%{__struct__: Ecto.Association.Has}), do: :has_many
  defp get_association_type(%{__struct__: Ecto.Association.BelongsTo}), do: :belongs_to
  defp get_association_type(%{__struct__: Ecto.Association.HasThrough}), do: :has_many_through
  defp get_association_type(_), do: :unknown

  defp build_associations(associations_info, joins_config) do
    joins_config
    |> Enum.filter(&Map.has_key?(associations_info, &1))
    |> Enum.into(%{}, fn join ->
      assoc = associations_info[join]
      {join, Map.put(assoc, :queryable, get_schema_atom(assoc.queryable))}
    end)
  end

  defp build_join_schemas(associations_info, joins_config) do
    joins_config
    |> Enum.filter(&Map.has_key?(associations_info, &1))
    |> Enum.filter(fn join ->
      # Only include joins for actual Ecto schema modules
      assoc = associations_info[join]
      is_ecto_schema?(assoc.queryable)
    end)
    |> Enum.into(%{}, fn join ->
      assoc = associations_info[join]
      schema_atom = get_schema_atom(assoc.queryable)
      
      # Introspect the associated schema
      assoc_schema_info = introspect_schema(assoc.queryable)
      
      schema_config = %{
        source_table: assoc_schema_info.table,
        primary_key: assoc_schema_info.primary_key,
        fields: assoc_schema_info.fields,
        redact_fields: [],
        columns: assoc_schema_info.columns,
        associations: %{}
      }
      
      {schema_atom, schema_config}
    end)
  end

  defp build_joins_definition(associations_info, joins_config) do
    joins_config
    |> Enum.filter(&Map.has_key?(associations_info, &1))
    |> Enum.into(%{}, fn join ->
      assoc = associations_info[join]
      
      join_config = %{
        name: humanize_atom(join),
        type: association_to_join_type(assoc.type)
      }
      
      {join, join_config}
    end)
  end

  defp association_to_join_type(:has_many), do: :left
  defp association_to_join_type(:belongs_to), do: :left
  defp association_to_join_type(:has_many_through), do: :left
  defp association_to_join_type(_), do: :left

  defp get_db_connection(repo) do
    # Return the repo itself instead of creating a separate Postgrex connection
    # This allows Selecto to use Ecto's connection pool
    repo
  end

  defp get_schema_name(schema) do
    schema
    |> Module.split()
    |> List.last()
  end

  defp get_schema_atom(schema) when is_atom(schema) do
    # Handle both module atoms and plain atoms
    schema_str = Atom.to_string(schema)
    if String.starts_with?(schema_str, "Elixir.") do
      # This is a module atom
      schema
      |> Module.split()
      |> List.last()
      |> Macro.underscore()
      |> String.to_atom()
    else
      # This is already a plain atom
      schema
    end
  end
  
  defp get_schema_atom(schema) when is_binary(schema) do
    String.to_atom(schema)
  end

  defp is_ecto_schema?(module) when is_atom(module) do
    try do
      # Check if it's a module that has __schema__/1 function
      module.__schema__(:source)
      true
    rescue
      _ -> false
    end
  end
  
  defp is_ecto_schema?(_), do: false

  defp humanize_atom(atom) when is_atom(atom) do
    atom
    |> Atom.to_string()
    |> String.replace("_", " ")
    |> String.split()
    |> Enum.map(&String.capitalize/1)
    |> Enum.join(" ")
  end
end