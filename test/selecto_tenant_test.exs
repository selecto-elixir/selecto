defmodule Selecto.TenantTest do
  use ExUnit.Case, async: true

  defp domain do
    %{
      name: "Accounts",
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

  defp selecto(domain_map),
    do: Selecto.configure(domain_map, [hostname: "localhost"], validate: false)

  test "with_tenant stores normalized context" do
    query =
      domain()
      |> selecto()
      |> Selecto.with_tenant(tenant_id: "acme", prefix: "tenant_acme")

    assert Selecto.tenant(query) == %{
             tenant_id: "acme",
             tenant_mode: nil,
             tenant_field: "tenant_id",
             prefix: "tenant_acme",
             namespace: "tenant",
             required_filters: []
           }
  end

  test "apply_tenant_scope adds tenant filter as required filter" do
    query =
      domain()
      |> selecto()
      |> Selecto.with_tenant(%{tenant_id: "acme"})
      |> Selecto.apply_tenant_scope()

    assert Selecto.required_filters(query) == [{"active", true}, {"tenant_id", "acme"}]
  end

  test "query_filters includes runtime required tenant filters" do
    query =
      domain()
      |> selecto()
      |> Selecto.require_tenant_filter("tenant_id", "acme")

    assert Selecto.query_filters(query) == [{"active", true}, {"tenant_id", "acme"}]
  end

  test "tenant prefix is merged into execute options when missing" do
    query =
      domain()
      |> selecto()
      |> Selecto.with_tenant(%{tenant_id: "acme", prefix: "tenant_acme"})

    assert Selecto.Tenant.merge_execution_opts(query, timeout: 1000) ==
             [prefix: "tenant_acme", timeout: 1000]

    assert Selecto.Tenant.merge_execution_opts(query, prefix: "explicit") ==
             [prefix: "explicit"]
  end

  test "tenant required filters appear in generated sql" do
    {sql, params} =
      domain()
      |> selecto()
      |> Selecto.select(["name"])
      |> Selecto.with_tenant(%{tenant_id: "acme"})
      |> Selecto.apply_tenant_scope()
      |> Selecto.to_sql()

    assert sql =~ ~r/where/i
    assert sql =~ "active"
    assert sql =~ "tenant_id"
    assert true in params
    assert "acme" in params
  end
end
