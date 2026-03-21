defmodule Selecto.QueryEagerValidationTest do
  use ExUnit.Case, async: true

  defp domain do
    %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :status, :total, :active, :tags],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          status: %{type: :string},
          total: %{type: :decimal},
          active: %{type: :boolean},
          tags: %{type: :array}
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
      },
      custom_columns: %{
        "status_label" => %{
          select: "UPPER(selecto_root.status)",
          type: :string
        }
      }
    }
  end

  defp selecto do
    Selecto.configure(domain(), :mock_connection, validate: false)
  end

  test "select validates plain, qualified, and custom fields eagerly" do
    query =
      selecto()
      |> Selecto.select(["status", "order_items.quantity", "status_label"])

    assert query.set.selected == ["status", "order_items.quantity", "status_label"]
  end

  test "select raises immediately for invalid fields" do
    assert_raise ArgumentError, ~r/missing_field/, fn ->
      selecto()
      |> Selecto.select(["missing_field"])
    end
  end

  test "aggregate selectors validate their field references eagerly" do
    query =
      selecto()
      |> Selecto.select([{:sum, "total"}])

    assert query.set.selected == [{:sum, "total"}]

    assert_raise ArgumentError, ~r/missing_total/, fn ->
      selecto()
      |> Selecto.select([{:sum, "missing_total"}])
    end
  end

  test "function selector args validate field references eagerly" do
    query =
      selecto()
      |> Selecto.select([{:func, "COALESCE", ["status", {:literal, "unknown"}]}])

    assert query.set.selected == [{:func, "COALESCE", ["status", {:literal, "unknown"}]}]

    assert_raise ArgumentError, ~r/missing_status/, fn ->
      selecto()
      |> Selecto.select([{:func, "COALESCE", ["missing_status", {:literal, "unknown"}]}])
    end
  end

  test "filter validates nested boolean filters eagerly" do
    query =
      selecto()
      |> Selecto.filter({:and, [{"status", "paid"}, {:not, {"order_items.quantity", {:gt, 1}}}]})

    assert query.set.filtered == [
             {:and, [{"status", "paid"}, {:not, {"order_items.quantity", {:gt, 1}}}]}
           ]
  end

  test "filter raises immediately for invalid nested fields" do
    assert_raise ArgumentError, ~r/missing_quantity/, fn ->
      selecto()
      |> Selecto.filter({:and, [{"status", "paid"}, {:not, {"missing_quantity", {:gt, 1}}}]})
    end
  end

  test "post retarget filter also validates fields eagerly" do
    assert_raise ArgumentError, ~r/bad_post_filter/, fn ->
      selecto()
      |> Selecto.post_retarget_filter({"bad_post_filter", 1})
    end
  end

  test "order_by validates fields eagerly" do
    query =
      selecto()
      |> Selecto.order_by({"order_items.quantity", :desc})

    assert query.set.order_by == [{"order_items.quantity", :desc}]

    assert_raise ArgumentError, ~r/missing_sort/, fn ->
      selecto()
      |> Selecto.order_by({"missing_sort", :asc})
    end
  end

  test "group_by validates fields eagerly including rollup" do
    query =
      selecto()
      |> Selecto.group_by(rollup: ["status"])

    assert query.set.group_by == [rollup: ["status"]]

    assert_raise ArgumentError, ~r/missing_group/, fn ->
      selecto()
      |> Selecto.group_by(rollup: ["missing_group"])
    end
  end

  test "cte-qualified fields validate eagerly after with_cte" do
    cte_query =
      selecto()
      |> Selecto.with_cte(
        "active_orders",
        fn ->
          selecto()
          |> Selecto.select(["id", "status"])
        end,
        columns: ["id", "status"]
      )
      |> Selecto.select(["active_orders.status"])

    assert "active_orders.status" in cte_query.set.selected
  end

  test "manual dynamic columns validate eagerly" do
    query =
      selecto()
      |> then(fn base ->
        update_in(base.set, &Map.put(&1, :dynamic_columns, %{"dyn_col" => true}))
      end)
      |> Selecto.select(["dyn_col"])

    assert query.set.selected == ["dyn_col"]
  end

  test "raw sql field aliases remain deferred for later query steps" do
    query =
      selecto()
      |> Selecto.select(["status", {:field, {:raw_sql, "product_tag"}, "product_tag"}])
      |> Selecto.unnest("tags", as: "product_tag")

    {sql, _params} = Selecto.to_sql(query)

    assert sql =~ "product_tag"
  end
end
