defmodule Selecto.CteQuery do
  @moduledoc false

  alias Selecto.QueryMembers

  def with_cte(selecto, member_id) when is_atom(member_id) or is_binary(member_id) do
    with_cte(selecto, member_id, [])
  end

  def with_cte(selecto, member_id, opts)
      when (is_atom(member_id) or is_binary(member_id)) and (is_list(opts) or is_map(opts)) do
    normalized_overrides = QueryMembers.normalize_opts(opts)
    {member_name, raw_spec} = QueryMembers.fetch!(selecto, :ctes, member_id)
    spec = QueryMembers.normalize_spec(raw_spec)

    join_opts =
      QueryMembers.merge_join_opts(
        Map.get(spec, :join),
        if(Keyword.has_key?(normalized_overrides, :join),
          do: Keyword.get(normalized_overrides, :join),
          else: :__missing__
        ),
        &QueryMembers.normalize_cte_join_opts/1
      )

    recursive? =
      Keyword.get(normalized_overrides, :type, Map.get(spec, :type)) == :recursive or
        not is_nil(Map.get(spec, :base_query)) or not is_nil(Map.get(spec, :recursive_query)) or
        Keyword.has_key?(normalized_overrides, :base_query) or
        Keyword.has_key?(normalized_overrides, :recursive_query)

    cte_name = Keyword.get(normalized_overrides, :name, Map.get(spec, :name, member_name))

    cte_spec =
      if recursive? do
        base_query =
          Keyword.get(normalized_overrides, :base_query, Map.get(spec, :base_query))
          |> QueryMembers.wrap_base_query!(selecto, member_name)

        recursive_query =
          Keyword.get(normalized_overrides, :recursive_query, Map.get(spec, :recursive_query))
          |> QueryMembers.wrap_recursive_query!(selecto, member_name)

        recursive_opts =
          []
          |> QueryMembers.maybe_put_keyword(
            :columns,
            Keyword.get(normalized_overrides, :columns, Map.get(spec, :columns))
          )
          |> QueryMembers.maybe_put_keyword(
            :dependencies,
            Keyword.get(normalized_overrides, :dependencies, Map.get(spec, :dependencies))
          )
          |> QueryMembers.maybe_put_keyword(
            :max_depth,
            Keyword.get(normalized_overrides, :max_depth, Map.get(spec, :max_depth))
          )
          |> QueryMembers.maybe_put_keyword(
            :cycle_detection,
            Keyword.get(normalized_overrides, :cycle_detection, Map.get(spec, :cycle_detection))
          )
          |> Keyword.put(:base_query, base_query)
          |> Keyword.put(:recursive_query, recursive_query)

        Selecto.Advanced.CTE.create_recursive_cte(to_string(cte_name), recursive_opts)
      else
        query_builder =
          Keyword.get(
            normalized_overrides,
            :query,
            Keyword.get(normalized_overrides, :query_builder)
          ) || Map.get(spec, :query, Map.get(spec, :query_builder))

        cte_opts =
          []
          |> QueryMembers.maybe_put_keyword(
            :columns,
            Keyword.get(normalized_overrides, :columns, Map.get(spec, :columns))
          )
          |> QueryMembers.maybe_put_keyword(
            :dependencies,
            Keyword.get(normalized_overrides, :dependencies, Map.get(spec, :dependencies))
          )

        Selecto.Advanced.CTE.create_cte(
          to_string(cte_name),
          QueryMembers.wrap_query_builder!(query_builder, selecto, :ctes, member_name),
          cte_opts
        )
      end

    join_opts = if join_opts == :__missing__, do: nil, else: join_opts

    selecto
    |> upsert_cte_spec(cte_spec)
    |> maybe_join_cte_spec(cte_spec, join_opts)
  end

  def with_cte(selecto, name, query_builder, opts \\ []) do
    join_opts = Keyword.get(opts, :join)
    cte_opts = Keyword.delete(opts, :join)

    cte_spec = Selecto.Advanced.CTE.create_cte(name, query_builder, cte_opts)

    selecto
    |> append_cte_spec(cte_spec)
    |> maybe_join_cte_spec(cte_spec, join_opts)
  end

  @doc """
  Add a recursive Common Table Expression (CTE) to the query.

  Recursive CTEs enable hierarchical queries by combining an anchor query
  with a recursive query that references the CTE itself.

  ## Parameters

  - `selecto` - The Selecto instance
  - `name` - CTE name (must be valid SQL identifier)
  - `opts` - Options with :base_query, :recursive_query, and optional :join

  Join shortcut options (`:join`) follow `with_cte/4`.

  ## Examples

      # Hierarchical employee structure
      selecto
      |> Selecto.with_recursive_cte("employee_hierarchy",
          base_query: fn ->
            # Anchor: top-level managers
            Selecto.configure(employee_domain, connection)
            |> Selecto.select(["employee_id", "name", "manager_id", {:literal, 0, as: "level"}])
            |> Selecto.filter([{"manager_id", nil}])
          end,
          recursive_query: fn cte_ref ->
            # Recursive: subordinates
            Selecto.configure(employee_domain, connection)
            |> Selecto.select(["employee.employee_id", "employee.name", "employee.manager_id",
                              {:func, "employee_hierarchy.level + 1", as: "level"}])
            |> Selecto.join(:inner, cte_ref, on: "employee.manager_id = employee_hierarchy.employee_id")
          end,
          join: [owner_key: :employee_id, related_key: :employee_id]
        )
      |> Selecto.select([
          "employee_hierarchy.employee_id",
          "employee_hierarchy.name",
          "employee_hierarchy.level"
        ])
      |> Selecto.order_by([{"employee_hierarchy.level", :asc}, {"employee_hierarchy.name", :asc}])

      # Generated SQL:
      # WITH RECURSIVE employee_hierarchy AS (
      #   SELECT employee_id, name, manager_id, 0 as level
      #   FROM employee
      #   WHERE manager_id IS NULL
      #   UNION ALL
      #   SELECT employee.employee_id, employee.name, employee.manager_id, employee_hierarchy.level + 1
      #   FROM employee
      #   INNER JOIN employee_hierarchy ON employee.manager_id = employee_hierarchy.employee_id
      # )
      # SELECT employee_hierarchy.employee_id, employee_hierarchy.name, employee_hierarchy.level
      # FROM employee
      # INNER JOIN employee_hierarchy ON employee.employee_id = employee_hierarchy.employee_id
      # ORDER BY employee_hierarchy.level ASC, employee_hierarchy.name ASC
  """
  # Consolidated version that handles both parameter formats:
  # 1. (selecto, cte_name, base_fn, recursive_fn, opts) - original inline format
  # 2. (selecto, name, opts) - newer format using Advanced.CTE
  def with_recursive_cte(selecto, arg2, arg3, arg4 \\ nil, arg5 \\ []) do
    {cte_spec, join_opts} =
      case {arg2, arg3, arg4, arg5} do
        # Format 1: (selecto, cte_name, base_fn, recursive_fn, opts)
        {cte_name, base_fn, recursive_fn, opts}
        when is_function(base_fn) and is_function(recursive_fn) ->
          normalized_opts = QueryMembers.normalize_cte_option_list(opts)
          join_opts = Keyword.get(normalized_opts, :join)
          cte_opts = Keyword.delete(normalized_opts, :join)

          # Inline spec format
          {%{
             name: cte_name,
             type: :recursive,
             base_query: base_fn,
             recursive_query: recursive_fn,
             max_depth: Keyword.get(cte_opts, :max_depth),
             cycle_detection: Keyword.get(cte_opts, :cycle_detection, false),
             columns: Keyword.get(cte_opts, :columns)
           }, join_opts}

        # Format 2: (selecto, name, opts)
        {name, opts, nil, []} when is_list(opts) or is_map(opts) ->
          normalized_opts = QueryMembers.normalize_cte_option_list(opts)
          join_opts = Keyword.get(normalized_opts, :join)
          cte_opts = Keyword.delete(normalized_opts, :join)

          # Use Advanced.CTE module
          {Selecto.Advanced.CTE.create_recursive_cte(name, cte_opts), join_opts}
      end

    selecto
    |> append_cte_spec(cte_spec)
    |> maybe_join_cte_spec(cte_spec, join_opts)
  end

  @doc """
  Add multiple CTEs to the query in a single operation.

  Useful for complex queries that require multiple temporary result sets.
  CTEs will be automatically ordered based on their dependencies.

  ## Parameters

  - `selecto` - The Selecto instance
  - `cte_specs` - List of CTE specifications created with create_cte/3
  - `opts` - Options including :joins for auto-joining one or more CTEs

  Batch join shortcut options (`:joins`):
  - `true` - auto-join every provided CTE (default key inference)
  - list of entries where each entry is one of:
    - cte name (`"my_cte"` or `:my_cte`) to use inferred defaults
    - `{name, join_opts}` tuple
    - keyword/map with `:name` plus join options

  ## Examples

      # Multiple related CTEs
      active_customers_cte = Selecto.Advanced.CTE.create_cte("active_customers", fn ->
        Selecto.configure(customer_domain, connection)
        |> Selecto.filter([{"active", true}])
      end)

      high_value_cte = Selecto.Advanced.CTE.create_cte("high_value_customers", fn ->
        Selecto.configure(customer_domain, connection)
        |> Selecto.aggregate([{"payment.amount", :sum, as: "total_spent"}])
        |> Selecto.join(:inner, "payment", on: "customer.customer_id = payment.customer_id")
        |> Selecto.group_by(["customer.customer_id"])
        |> Selecto.having([{"total_spent", {:>, 100}}])
      end, dependencies: ["active_customers"])

      selecto
      |> Selecto.with_ctes([active_customers_cte, high_value_cte],
        joins: [
          [name: "active_customers", owner_key: :customer_id, related_key: :customer_id],
          [name: "high_value_customers", owner_key: :customer_id, related_key: :customer_id]
        ]
      )
      |> Selecto.select(["film.title", "high_value_customers.total_spent"])
  """
  def with_ctes(selecto, cte_specs, opts \\ []) when is_list(cte_specs) do
    selecto_with_ctes = append_cte_specs(selecto, cte_specs)
    apply_with_ctes_joins(selecto_with_ctes, cte_specs, Keyword.get(opts, :joins))
  end

  defp append_cte_spec(selecto, cte_spec), do: append_cte_specs(selecto, [cte_spec])

  defp append_cte_specs(selecto, cte_specs) when is_list(cte_specs) do
    current_ctes = Map.get(selecto.set, :ctes, [])
    updated_ctes = current_ctes ++ cte_specs

    put_in(selecto.set[:ctes], updated_ctes)
  end

  defp upsert_cte_spec(selecto, cte_spec) do
    cte_name = cte_spec_name!(cte_spec)
    current_ctes = Map.get(selecto.set, :ctes, [])

    updated_ctes =
      current_ctes
      |> Enum.reject(fn existing -> cte_spec_name(existing) == cte_name end)
      |> Kernel.++([cte_spec])

    put_in(selecto.set[:ctes], updated_ctes)
  end

  defp maybe_join_cte_spec(selecto, _cte_spec, join_opts) when join_opts in [nil, false],
    do: selecto

  defp maybe_join_cte_spec(selecto, cte_spec, join_opts) do
    {join_id, join_options} = build_cte_join_options(cte_spec, join_opts)
    Selecto.join(selecto, join_id, join_options)
  end

  defp build_cte_join_options(cte_spec, true), do: build_cte_join_options(cte_spec, [])

  defp build_cte_join_options(cte_spec, join_opts) when is_map(join_opts) do
    cte_spec
    |> build_cte_join_options(QueryMembers.normalize_cte_join_opts(join_opts))
  end

  defp build_cte_join_options(cte_spec, join_opts) when is_list(join_opts) do
    cte_name = cte_spec_name!(cte_spec)
    normalized_join_opts = QueryMembers.normalize_cte_join_opts(join_opts)

    join_id =
      normalized_join_opts
      |> Keyword.get(:id, cte_name)
      |> QueryMembers.normalize_values_join_id()

    join_options =
      normalized_join_opts
      |> Keyword.delete(:id)
      |> Keyword.delete(:name)
      |> Keyword.put_new(:source, cte_name)
      |> Keyword.put_new(:type, :left)
      |> maybe_add_default_cte_join_keys(cte_spec)
      |> maybe_add_default_cte_join_fields(cte_spec)

    {join_id, join_options}
  end

  defp build_cte_join_options(_cte_spec, invalid_opts) do
    raise ArgumentError,
          ":join option for CTE helpers must be true, false, nil, a keyword list, or a map. Got: #{inspect(invalid_opts)}"
  end

  defp maybe_add_default_cte_join_keys(join_options, cte_spec) do
    if Keyword.has_key?(join_options, :on) do
      join_options
    else
      case {Keyword.get(join_options, :owner_key), Keyword.get(join_options, :related_key)} do
        {owner_key, related_key} when not is_nil(owner_key) and not is_nil(related_key) ->
          join_options

        {owner_key, nil} when not is_nil(owner_key) ->
          Keyword.put(join_options, :related_key, owner_key)

        _ ->
          default_join_key = default_cte_join_key(cte_spec)
          owner_key = Keyword.get(join_options, :owner_key, default_join_key)
          related_key = Keyword.get(join_options, :related_key, owner_key)

          join_options
          |> Keyword.put_new(:owner_key, owner_key)
          |> Keyword.put_new(:related_key, related_key)
      end
    end
  end

  defp maybe_add_default_cte_join_fields(join_options, cte_spec) do
    case Keyword.get(join_options, :fields, :__missing__) do
      :infer ->
        Keyword.put(join_options, :fields, inferred_cte_join_fields!(cte_spec))

      :__missing__ ->
        inferred_fields = inferred_cte_join_fields(cte_spec)

        if inferred_fields == %{} do
          join_options
        else
          Keyword.put(join_options, :fields, inferred_fields)
        end

      _ ->
        join_options
    end
  end

  defp default_cte_join_key(cte_spec) do
    case cte_columns(cte_spec) do
      [first_column | _] ->
        first_column

      [] ->
        cte_name = cte_spec_name!(cte_spec)

        raise ArgumentError,
              "Cannot infer join keys for CTE '#{cte_name}'. Declare CTE columns or pass :owner_key/:related_key (or :on) in :join options."
    end
  end

  defp inferred_cte_join_fields(cte_spec) do
    Enum.reduce(cte_columns(cte_spec), %{}, fn column, acc ->
      Map.put(acc, String.to_atom(column), %{type: :any})
    end)
  end

  defp inferred_cte_join_fields!(cte_spec) do
    inferred_fields = inferred_cte_join_fields(cte_spec)

    if inferred_fields == %{} do
      cte_name = cte_spec_name!(cte_spec)

      raise ArgumentError,
            "Cannot infer fields for CTE '#{cte_name}' because it has no declared columns. Provide fields explicitly or declare CTE columns."
    else
      inferred_fields
    end
  end

  defp cte_columns(cte_spec) do
    cte_spec
    |> Map.get(:columns, [])
    |> List.wrap()
    |> Enum.map(&to_string/1)
  end

  defp cte_spec_name(%{name: name}) when is_binary(name), do: name
  defp cte_spec_name(%{name: name}) when is_atom(name), do: Atom.to_string(name)
  defp cte_spec_name(_cte_spec), do: nil

  defp cte_spec_name!(%{name: name}) when is_binary(name), do: name
  defp cte_spec_name!(%{name: name}) when is_atom(name), do: Atom.to_string(name)

  defp cte_spec_name!(cte_spec) do
    raise ArgumentError,
          "CTE spec must include a string or atom :name. Got: #{inspect(cte_spec)}"
  end

  defp apply_with_ctes_joins(selecto, _cte_specs, joins) when joins in [nil, false, []],
    do: selecto

  defp apply_with_ctes_joins(selecto, cte_specs, true) do
    Enum.reduce(cte_specs, selecto, fn cte_spec, acc ->
      maybe_join_cte_spec(acc, cte_spec, true)
    end)
  end

  defp apply_with_ctes_joins(selecto, cte_specs, joins) when is_list(joins) do
    Enum.reduce(joins, selecto, fn join_entry, acc ->
      {cte_spec, join_opts} = resolve_with_ctes_join_entry(cte_specs, join_entry)
      maybe_join_cte_spec(acc, cte_spec, join_opts)
    end)
  end

  defp apply_with_ctes_joins(_selecto, _cte_specs, invalid_joins) do
    raise ArgumentError,
          ":joins option for with_ctes/3 must be true, false, nil, or a list. Got: #{inspect(invalid_joins)}"
  end

  defp resolve_with_ctes_join_entry(cte_specs, entry) when is_binary(entry) or is_atom(entry) do
    {find_cte_spec!(cte_specs, entry), true}
  end

  defp resolve_with_ctes_join_entry(cte_specs, {name, join_opts}) do
    {find_cte_spec!(cte_specs, name), join_opts}
  end

  defp resolve_with_ctes_join_entry(cte_specs, join_entry) when is_map(join_entry) do
    resolve_with_ctes_join_entry(cte_specs, Map.to_list(join_entry))
  end

  defp resolve_with_ctes_join_entry(cte_specs, join_entry) when is_list(join_entry) do
    normalized_entry = QueryMembers.normalize_cte_join_opts(join_entry)
    cte_name = Keyword.get(normalized_entry, :name)

    if is_nil(cte_name) do
      raise ArgumentError,
            "Each entry in :joins for with_ctes/3 must include :name, be a cte name, or be a {name, opts} tuple. Got: #{inspect(join_entry)}"
    end

    join_opts = Keyword.delete(normalized_entry, :name)
    {find_cte_spec!(cte_specs, cte_name), join_opts}
  end

  defp resolve_with_ctes_join_entry(_cte_specs, invalid_entry) do
    raise ArgumentError,
          "Invalid :joins entry for with_ctes/3: #{inspect(invalid_entry)}"
  end

  defp find_cte_spec!(cte_specs, cte_name) do
    cte_name_string = to_string(cte_name)

    Enum.find(cte_specs, fn cte_spec ->
      cte_spec_name!(cte_spec) == cte_name_string
    end) ||
      raise ArgumentError,
            "CTE named '#{cte_name_string}' was not found in with_ctes/3 input"
  end
end
