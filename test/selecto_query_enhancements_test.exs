defmodule Selecto.QueryEnhancementsTest do
  use ExUnit.Case, async: true

  defp domain do
    %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :order_number, :status, :total, :customer_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          order_number: %{type: :string},
          status: %{type: :string},
          total: %{type: :decimal},
          customer_id: %{type: :integer}
        },
        associations: %{
          order_items: %{
            queryable: :order_items,
            field: :order_items,
            owner_key: :id,
            related_key: :order_id
          }
        }
      },
      schemas: %{
        order_items: %{
          source_table: "order_items",
          primary_key: :id,
          fields: [:id, :order_id, :quantity],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            order_id: %{type: :integer},
            quantity: %{type: :integer}
          },
          associations: %{}
        }
      },
      joins: %{
        order_items: %{
          type: :left,
          name: "order_items"
        }
      }
    }
  end

  defp product_domain do
    %{
      name: "Products",
      source: %{
        source_table: "products",
        primary_key: :id,
        fields: [:id, :name, :price, :metadata],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          price: %{type: :decimal},
          metadata: %{type: :jsonb}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp selecto(domain_map),
    do: Selecto.configure(domain_map, [hostname: "localhost"], validate: false)

  test "to_sql supports pretty formatting and highlighting" do
    query =
      domain()
      |> selecto()
      |> Selecto.select(["order_number", "status", "total"])
      |> Selecto.filter({"status", "delivered"})
      |> Selecto.order_by({"total", :desc})

    {plain_sql, _plain_params} = Selecto.to_sql(query)
    {pretty_sql, _pretty_params} = Selecto.to_sql(query, pretty: true)
    {highlighted_sql, _hl_params} = Selecto.to_sql(query, pretty: true, highlight: :ansi)

    assert is_binary(plain_sql)
    assert String.starts_with?(pretty_sql, "SELECT")
    assert String.contains?(pretty_sql, "\nFROM")
    assert String.contains?(pretty_sql, "\nWHERE")
    assert String.contains?(highlighted_sql, "\e[")
  end

  test "pre_pivot_filter and post_pivot_filter update explicit filter buckets" do
    query =
      domain()
      |> selecto()
      |> Selecto.pre_pivot_filter({"status", "delivered"})
      |> Selecto.pivot(:order_items)
      |> Selecto.post_pivot_filter({"order_items.quantity", {:gt, 2}})

    assert query.set.filtered == [{"status", "delivered"}]
    assert query.set.post_pivot_filters == [{"order_items.quantity", {:gt, 2}}]
  end

  test "query_filters exposes unified filters with optional post-pivot inclusion" do
    query =
      domain()
      |> selecto()
      |> Selecto.pre_pivot_filter({"status", "delivered"})
      |> Selecto.pivot(:order_items)
      |> Selecto.post_pivot_filter({"order_items.quantity", {:gt, 2}})

    assert Selecto.pre_pivot_filters(query) == [{"status", "delivered"}]
    assert Selecto.post_pivot_filters(query) == [{"order_items.quantity", {:gt, 2}}]

    assert Selecto.query_filters(query) == [
             {"status", "delivered"},
             {"order_items.quantity", {:gt, 2}}
           ]

    assert Selecto.query_filters(query, include_post_pivot: false) == [
             {"status", "delivered"}
           ]
  end

  test "missing field error includes computed alias hint" do
    query =
      product_domain()
      |> selecto()
      |> Selecto.json_select([{:json_extract_text, "metadata", "$.price_band", as: "price_band"}])
      |> Selecto.select(["price_band"])

    assert_raise RuntimeError, ~r/matches a computed alias/, fn ->
      Selecto.to_sql(query)
    end
  end

  test "select_shape expands json alias leaf into explicit field selector" do
    query =
      product_domain()
      |> selecto()
      |> Selecto.json_select([{:json_extract_text, "metadata", "$.price_band", as: "price_band"}])
      |> Selecto.select_shape(["name", "price_band"])

    assert {:field, "metadata.price_band", "price_band"} in query.set.selected
    assert "name" in query.set.selected
  end

  test "diagnostics builds EXPLAIN SQL with selected flags" do
    explain_sql =
      Selecto.Diagnostics.build_explain_sql(
        "SELECT 1",
        analyze: true,
        buffers: true,
        timing: false,
        format: :json
      )

    assert String.starts_with?(explain_sql, "EXPLAIN (")
    assert String.contains?(explain_sql, "ANALYZE")
    assert String.contains?(explain_sql, "BUFFERS")
    assert String.contains?(explain_sql, "TIMING false")
    assert String.contains?(explain_sql, "FORMAT JSON")
  end
end
