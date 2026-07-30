defmodule Selecto.Verification.QuerySafety do
  @moduledoc """
  Bounded-exhaustive verification of Selecto read-path scope invariants.

  The model enumerates tenant-required and tenant-optional domains, absent,
  row-scoped, and schema-scoped tenant contexts, explicit scope application,
  and ordinary user filters. It checks fail-closed validation, preservation of
  required filters, and SQL parameterization for every state.

  A passing result is a proof over this finite model, not a claim about all
  possible Selecto programs. See `Selecto.Verification.BoundedModel`.
  """

  alias Selecto.Verification.BoundedModel

  @contexts [:none, :tenant_id, :prefix]

  @doc """
  Runs the built-in read-path safety model.
  """
  @spec verify() :: BoundedModel.report()
  def verify do
    BoundedModel.check("selecto.query_scope.v1", states(), invariants())
  end

  @doc false
  def states do
    for tenant_required? <- [false, true],
        context <- @contexts,
        apply_scope? <- [false, true],
        user_filter? <- [false, true] do
      build_state(tenant_required?, context, apply_scope?, user_filter?)
    end
  end

  defp invariants do
    [
      {"required_scope_fails_closed", &required_scope_fails_closed/1},
      {"required_filters_are_preserved", &required_filters_are_preserved/1},
      {"row_scope_is_parameterized", &row_scope_is_parameterized/1},
      {"user_filters_cannot_replace_required_scope",
       &user_filters_cannot_replace_required_scope/1}
    ]
  end

  defp build_state(tenant_required?, context, apply_scope?, user_filter?) do
    query =
      tenant_required?
      |> domain()
      |> Selecto.configure([hostname: "verification.invalid"], validate: false)
      |> Selecto.select(["name"])
      |> attach_context(context, tenant_required?)
      |> maybe_apply_scope(apply_scope?)
      |> maybe_add_user_filter(user_filter?)

    %{
      tenant_required?: tenant_required?,
      context: context,
      apply_scope?: apply_scope?,
      user_filter?: user_filter?,
      query: query
    }
  end

  defp required_scope_fails_closed(state) do
    expected_valid? = expected_valid?(state)
    actual_valid? = Selecto.validate_tenant_scope(state.query) == :ok

    if actual_valid? == expected_valid?,
      do: :ok,
      else: {:error, %{expected_valid?: expected_valid?, actual_valid?: actual_valid?}}
  end

  defp required_filters_are_preserved(state) do
    if expected_valid?(state) do
      required = Selecto.required_filters(state.query)
      actual = Selecto.query_filters(state.query)
      missing = required -- actual

      if missing == [], do: :ok, else: {:error, %{missing_filters: missing}}
    else
      :ok
    end
  end

  defp row_scope_is_parameterized(%{context: :tenant_id, apply_scope?: true} = state) do
    {sql, params} = Selecto.to_sql(state.query)

    cond do
      not String.contains?(String.downcase(sql), "tenant_id") ->
        {:error, :tenant_field_missing_from_sql}

      "tenant-a" not in params ->
        {:error, %{tenant_value_missing_from_params: params}}

      String.contains?(sql, "tenant-a") ->
        {:error, :tenant_value_interpolated_into_sql}

      true ->
        :ok
    end
  end

  defp row_scope_is_parameterized(_state), do: :ok

  defp user_filters_cannot_replace_required_scope(
         %{
           tenant_required?: true,
           context: :none,
           user_filter?: true
         } = state
       ) do
    case Selecto.validate_tenant_scope(state.query) do
      {:error, %Selecto.Error{}} -> :ok
      other -> {:error, %{ordinary_filter_satisfied_tenant_scope: other}}
    end
  end

  defp user_filters_cannot_replace_required_scope(_state), do: :ok

  defp expected_valid?(%{tenant_required?: false}), do: true
  defp expected_valid?(%{context: :prefix}), do: true
  defp expected_valid?(%{context: :tenant_id, apply_scope?: true}), do: true
  defp expected_valid?(_state), do: false

  defp attach_context(query, :none, _tenant_required?), do: query

  defp attach_context(query, :tenant_id, tenant_required?) do
    Selecto.with_tenant(query, %{tenant_id: "tenant-a", required: tenant_required?})
  end

  defp attach_context(query, :prefix, tenant_required?) do
    Selecto.with_tenant(query, %{prefix: "tenant_a", required: tenant_required?})
  end

  defp maybe_apply_scope(query, true), do: Selecto.apply_tenant_scope(query)
  defp maybe_apply_scope(query, false), do: query

  defp maybe_add_user_filter(query, true), do: Selecto.filter(query, {"name", "Ada"})
  defp maybe_add_user_filter(query, false), do: query

  defp domain(tenant_required?) do
    %{
      name: "Verification Accounts",
      tenant_required: tenant_required?,
      source: %{
        source_table: "accounts",
        primary_key: :id,
        fields: [:id, :name, :active, :tenant_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          active: %{type: :boolean},
          tenant_id: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      required_filters: [{"active", true}]
    }
  end
end
