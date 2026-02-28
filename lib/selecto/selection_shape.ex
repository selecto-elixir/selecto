defmodule Selecto.SelectionShape do
  @moduledoc """
  Structured selection helpers for building and materializing nested result shapes.

  This module lets callers provide a list/tuple selection shape and receive rows
  back in that same nested shape via `execute_shape/2`.

  Nested lists/tuples that exclusively reference fields from a single joined
  schema are treated as subselect nodes and compiled to `json_agg` correlated
  subqueries.
  """

  alias Selecto.Error

  @type shape_input :: list() | tuple()

  @doc """
  Compile a nested selection shape into regular Selecto selectors + subselect specs.

  The generated execution plan is stored on `selecto.set.selection_shape` and can
  be used with `execute_shape/2`.
  """
  @spec select_shape(Selecto.t(), shape_input()) :: Selecto.t()
  def select_shape(selecto, shape) when is_list(shape) or is_tuple(shape) do
    initial_state = %{selected: [], subselects: [], alias_counter: 1}

    {root_node, state} = parse_node(selecto, shape, initial_state, false)

    plan = %{
      root: root_node,
      selected_count: length(state.selected),
      subselect_count: length(state.subselects)
    }

    updated_set =
      selecto.set
      |> Map.put(:selected, state.selected)
      |> Map.put(:subselected, state.subselects)
      |> Map.put(:selection_shape, plan)

    %{selecto | set: updated_set}
  end

  @doc """
  Execute a shape query and materialize rows into the configured nested shape.

  Returns `{:ok, shaped_rows}`.
  """
  @spec execute_shape(Selecto.t(), keyword()) :: {:ok, list()} | {:error, Selecto.Error.t()}
  def execute_shape(selecto, opts \\ []) do
    case Map.get(selecto.set, :selection_shape) do
      nil ->
        {:error,
         Error.validation_error(
           "No shape plan found. Call Selecto.select_shape/2 before execute_shape/2",
           %{}
         )}

      plan ->
        execute_opts = Keyword.put(opts, :format, :raw)

        case Selecto.execute(selecto, execute_opts) do
          {:ok, {rows, _columns, _aliases}} ->
            {:ok, shape_rows(rows, plan)}

          {:error, _} = error ->
            error
        end
    end
  end

  @doc """
  Materialize DB rows according to a compiled shape plan.
  """
  @spec shape_rows(list(), map()) :: list()
  def shape_rows(rows, %{root: root, selected_count: selected_count}) when is_list(rows) do
    Enum.map(rows, fn row -> materialize_node(row, root, selected_count) end)
  end

  # Parsing

  defp parse_node(selecto, node, state, allow_subselect?) when is_list(node) do
    maybe_parse_container(selecto, :list, node, state, allow_subselect?)
  end

  defp parse_node(selecto, node, state, allow_subselect?) when is_tuple(node) do
    if tuple_container?(node) do
      maybe_parse_container(selecto, :tuple, Tuple.to_list(node), state, allow_subselect?)
    else
      parse_leaf(node, state)
    end
  end

  defp parse_node(_selecto, node, state, _allow_subselect?) do
    parse_leaf(node, state)
  end

  defp maybe_parse_container(selecto, kind, children, state, allow_subselect?) do
    if allow_subselect? do
      case build_subselect_node(selecto, kind, children, state) do
        {:ok, node, next_state} ->
          {node, next_state}

        :not_subselect ->
          parse_regular_container(selecto, kind, children, state)
      end
    else
      parse_regular_container(selecto, kind, children, state)
    end
  end

  defp parse_regular_container(selecto, kind, children, state) do
    {child_nodes, final_state} =
      Enum.reduce(children, {[], state}, fn child, {nodes, acc_state} ->
        {child_node, next_state} = parse_node(selecto, child, acc_state, true)
        {nodes ++ [child_node], next_state}
      end)

    {{:container, kind, child_nodes}, final_state}
  end

  defp parse_leaf(selector, state) do
    index = length(state.selected)
    next_state = %{state | selected: state.selected ++ [selector]}
    {{:leaf, index}, next_state}
  end

  defp build_subselect_node(selecto, kind, children, state) do
    case collect_subselect_candidate(selecto, kind, children) do
      {:ok, %{target_schema: target_schema, fields: fields, template: template}} ->
        alias_name = "_shape_sub_#{state.alias_counter}"

        subselect_config = %{
          fields: fields,
          target_schema: target_schema,
          format: :json_agg,
          alias: alias_name,
          order_by: [],
          filters: []
        }

        subselect_index = length(state.subselects)

        next_state =
          state
          |> Map.update!(:subselects, fn subselects -> subselects ++ [subselect_config] end)
          |> Map.update!(:alias_counter, &(&1 + 1))

        single_field =
          case fields do
            [one] -> one
            _ -> nil
          end

        {:ok, {:subselect, subselect_index, template, single_field}, next_state}

      :not_subselect ->
        :not_subselect
    end
  end

  defp collect_subselect_candidate(_selecto, _kind, []), do: :not_subselect

  defp collect_subselect_candidate(selecto, kind, children) do
    case collect_candidate_nodes(selecto, children) do
      {:ok, %{target_schema: target_schema, fields: fields, templates: templates}} ->
        {:ok,
         %{
           target_schema: target_schema,
           fields: dedupe_preserve_order(fields),
           template: {:container, kind, templates}
         }}

      :not_subselect ->
        :not_subselect
    end
  end

  defp collect_candidate_nodes(selecto, nodes) do
    Enum.reduce_while(nodes, {:ok, %{target_schema: nil, fields: [], templates: []}}, fn node,
                                                                                         {:ok,
                                                                                          acc} ->
      case collect_candidate_node(selecto, node) do
        {:ok, %{target_schema: schema, fields: fields, template: template}} ->
          cond do
            is_nil(acc.target_schema) or acc.target_schema == schema ->
              updated = %{
                target_schema: schema,
                fields: acc.fields ++ fields,
                templates: acc.templates ++ [template]
              }

              {:cont, {:ok, updated}}

            true ->
              {:halt, :not_subselect}
          end

        :not_subselect ->
          {:halt, :not_subselect}
      end
    end)
    |> case do
      {:ok, %{target_schema: nil}} -> :not_subselect
      {:ok, %{fields: []}} -> :not_subselect
      {:ok, candidate} -> {:ok, candidate}
      :not_subselect -> :not_subselect
    end
  end

  defp collect_candidate_node(selecto, node) when is_list(node) do
    case collect_candidate_nodes(selecto, node) do
      {:ok, %{target_schema: schema, fields: fields, templates: templates}} ->
        {:ok, %{target_schema: schema, fields: fields, template: {:container, :list, templates}}}

      :not_subselect ->
        :not_subselect
    end
  end

  defp collect_candidate_node(selecto, node) when is_tuple(node) do
    if tuple_container?(node) do
      node
      |> Tuple.to_list()
      |> then(fn children -> collect_candidate_nodes(selecto, children) end)
      |> case do
        {:ok, %{target_schema: schema, fields: fields, templates: templates}} ->
          {:ok,
           %{target_schema: schema, fields: fields, template: {:container, :tuple, templates}}}

        :not_subselect ->
          :not_subselect
      end
    else
      collect_candidate_leaf(selecto, node)
    end
  end

  defp collect_candidate_node(selecto, node) do
    collect_candidate_leaf(selecto, node)
  end

  defp collect_candidate_leaf(selecto, leaf) do
    with {:ok, field_ref} <- extract_field_reference(leaf),
         %{} = field_config <- Selecto.field(selecto, field_ref),
         target_schema <- Map.get(field_config, :requires_join),
         true <- subselect_join?(target_schema) do
      field_name = field_name_from_config(field_config, field_ref)

      {:ok,
       %{
         target_schema: target_schema,
         fields: [field_name],
         template: {:item_field, field_name}
       }}
    else
      _ ->
        :not_subselect
    end
  end

  defp subselect_join?(join_name) when join_name in [nil, :selecto_root, "selecto_root"],
    do: false

  defp subselect_join?(join_name) when is_atom(join_name), do: true
  defp subselect_join?(_), do: false

  defp field_name_from_config(field_config, field_ref) do
    cond do
      is_binary(field_config[:field]) ->
        field_config[:field]

      is_atom(field_config[:field]) ->
        Atom.to_string(field_config[:field])

      is_binary(field_config[:name]) ->
        field_config[:name]

      is_atom(field_config[:name]) ->
        Atom.to_string(field_config[:name])

      true ->
        fallback_field_name(field_ref)
    end
  end

  defp fallback_field_name(field_ref) do
    field_ref
    |> to_string()
    |> String.split(".")
    |> List.last()
  end

  defp extract_field_reference(selector) when is_binary(selector), do: {:ok, selector}

  defp extract_field_reference(selector) when is_atom(selector),
    do: {:ok, Atom.to_string(selector)}

  defp extract_field_reference({:field, selector}) when is_binary(selector), do: {:ok, selector}

  defp extract_field_reference({:field, selector}) when is_atom(selector),
    do: {:ok, Atom.to_string(selector)}

  defp extract_field_reference({:field, selector, _alias}) when is_binary(selector),
    do: {:ok, selector}

  defp extract_field_reference({:field, selector, _alias}) when is_atom(selector),
    do: {:ok, Atom.to_string(selector)}

  defp extract_field_reference(_selector), do: :error

  defp tuple_container?(tuple) do
    if tuple_size(tuple) == 0 do
      true
    else
      not is_atom(elem(tuple, 0))
    end
  end

  defp dedupe_preserve_order(values) do
    Enum.reduce(values, [], fn value, acc ->
      if value in acc do
        acc
      else
        acc ++ [value]
      end
    end)
  end

  # Materialization

  defp materialize_node(row, {:leaf, index}, _selected_count) do
    Enum.at(row, index)
  end

  defp materialize_node(row, {:container, :list, children}, selected_count) do
    Enum.map(children, fn child -> materialize_node(row, child, selected_count) end)
  end

  defp materialize_node(row, {:container, :tuple, children}, selected_count) do
    children
    |> Enum.map(fn child -> materialize_node(row, child, selected_count) end)
    |> List.to_tuple()
  end

  defp materialize_node(
         row,
         {:subselect, subselect_index, template, single_field},
         selected_count
       ) do
    value = Enum.at(row, selected_count + subselect_index)
    materialize_subselect(value, template, single_field)
  end

  defp materialize_subselect(nil, _template, _single_field), do: []

  defp materialize_subselect(value, template, single_field) do
    value
    |> normalize_subselect_items()
    |> Enum.map(fn item -> materialize_subselect_item(item, template, single_field) end)
  end

  defp normalize_subselect_items(items) when is_list(items), do: items

  defp normalize_subselect_items(items) when is_binary(items) do
    decode_subselect_json(items)
  end

  defp normalize_subselect_items(item), do: [item]

  defp decode_subselect_json(json) do
    if Code.ensure_loaded?(Jason) do
      case Jason.decode(json) do
        {:ok, decoded} when is_list(decoded) -> decoded
        {:ok, decoded} -> [decoded]
        {:error, _} -> []
      end
    else
      []
    end
  end

  defp materialize_subselect_item(item, {:item_field, field_name}, single_field) do
    case item do
      map when is_map(map) -> map_lookup_by_string_key(map, field_name)
      scalar -> if single_field == field_name, do: scalar, else: nil
    end
  end

  defp materialize_subselect_item(item, {:container, :list, children}, single_field) do
    Enum.map(children, fn child -> materialize_subselect_item(item, child, single_field) end)
  end

  defp materialize_subselect_item(item, {:container, :tuple, children}, single_field) do
    children
    |> Enum.map(fn child -> materialize_subselect_item(item, child, single_field) end)
    |> List.to_tuple()
  end

  defp map_lookup_by_string_key(map, field_name) when is_map(map) do
    cond do
      Map.has_key?(map, field_name) ->
        Map.get(map, field_name)

      true ->
        map
        |> Enum.find(fn {key, _value} -> to_string(key) == field_name end)
        |> case do
          {_, value} -> value
          nil -> nil
        end
    end
  end
end
