defmodule Selecto.QueryEagerValidationTest do
  use ExUnit.Case, async: true

  defp domain do
    %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :status, :total, :active, :tags, :metadata],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          status: %{type: :string},
          total: %{type: :decimal},
          active: %{type: :boolean},
          tags: %{type: :array},
          metadata: %{type: :jsonb}
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

  test "cte-backed join aliases validate eagerly after manual join" do
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
      |> Selecto.join(:active_order_lookup,
        source: "active_orders",
        owner_key: :id,
        related_key: :id,
        type: :left
      )
      |> Selecto.select(["active_order_lookup.status"])
      |> Selecto.filter({"active_order_lookup.status", {:not, nil}})
      |> Selecto.group_by(["active_order_lookup.status"])
      |> Selecto.order_by({"active_order_lookup.status", :asc})

    assert "active_order_lookup.status" in cte_query.set.selected
    assert {"active_order_lookup.status", {:not, nil}} in cte_query.set.filtered
    assert "active_order_lookup.status" in cte_query.set.group_by
    assert {"active_order_lookup.status", :asc} in cte_query.set.order_by
  end

  test "dynamic join raises immediately for missing explicit CTE source" do
    assert_raise ArgumentError,
                 ~r/Join 'missing_lookup' references CTE source 'missing_orders' but no such CTE is registered/,
                 fn ->
                   selecto()
                   |> Selecto.join(:missing_lookup,
                     source: "missing_orders",
                     source_kind: :cte,
                     owner_key: :id,
                     related_key: :id,
                     type: :left
                   )
                 end
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

  test "array predicate filters validate their field references eagerly" do
    query =
      selecto()
      |> Selecto.filter({:array_contains, "tags", ["featured"]})

    assert query.set.filtered == [{:array_contains, "tags", ["featured"]}]

    assert_raise ArgumentError, ~r/missing_tags/, fn ->
      selecto()
      |> Selecto.filter({:array_overlap, "missing_tags", ["featured"]})
    end
  end

  test "json path selectors and filters validate eagerly against jsonb roots" do
    query =
      selecto()
      |> Selecto.select(["metadata.warehouse.zone"])
      |> Selecto.filter({"metadata.warehouse.zone", :exists})

    assert query.set.selected == ["metadata.warehouse.zone"]
    assert query.set.filtered == [{"metadata.warehouse.zone", :exists}]
  end

  test "raw sql group specs are not misclassified as keyword group wrappers" do
    query =
      selecto()
      |> Selecto.group_by([{:raw_sql, "UPPER(selecto_root.status)"}])

    assert query.set.group_by == [{:raw_sql, "UPPER(selecto_root.status)"}]
  end

  test "raw sql field aliases remain deferred for later query steps" do
    query =
      selecto()
      |> Selecto.select(["status", {:field, {:raw_sql, "product_tag"}, "product_tag"}])
      |> Selecto.unnest("tags", as: "product_tag")

    {sql, _params} = Selecto.to_sql(query)

    assert sql =~ "product_tag"
    refute sql =~ "nil.product_tag"
  end

  test "window functions validate argument and over fields eagerly" do
    query =
      selecto()
      |> Selecto.window_function(:lag, ["total", 1],
        over: [partition_by: ["status"], order_by: ["order_items.quantity"]],
        as: "prev_total"
      )

    assert [%Selecto.Window.Spec{alias: "prev_total", arguments: ["total", 1]}] =
             query.set.window_functions

    assert_raise ArgumentError, ~r/missing_window_field/, fn ->
      selecto()
      |> Selecto.window_function(:lag, ["missing_window_field", 1], over: [order_by: ["status"]])
    end

    assert_raise ArgumentError, ~r/missing_window_partition/, fn ->
      selecto()
      |> Selecto.window_function(:sum, ["total"],
        over: [partition_by: ["missing_window_partition"]]
      )
    end

    assert_raise ArgumentError, ~r/missing_window_order/, fn ->
      selecto()
      |> Selecto.window_function(:sum, ["total"], over: [order_by: ["missing_window_order"]])
    end
  end

  test "unnest and JSON rowset helpers validate source fields eagerly" do
    assert_raise ArgumentError, ~r/missing_array_source/, fn ->
      selecto()
      |> Selecto.unnest("missing_array_source", as: "product_tag")
    end

    assert_raise ArgumentError, ~r/missing_json_source/, fn ->
      selecto()
      |> Selecto.json_table("missing_json_source",
        as: "item_rows",
        columns: [sku: "$.sku"]
      )
    end

    assert_raise ArgumentError, ~r/missing_json_rowset_source/, fn ->
      selecto()
      |> Selecto.json_rowset("missing_json_rowset_source", as: "item_rows", path: "$[*]")
    end
  end

  test "json helper builders validate source fields eagerly" do
    assert_raise ArgumentError, ~r/missing_json_select/, fn ->
      selecto()
      |> Selecto.json_select(
        {:json_extract_text, "missing_json_select", "$.warehouse.zone", as: "zone"}
      )
    end

    assert_raise ArgumentError, ~r/missing_json_filter/, fn ->
      selecto()
      |> Selecto.json_filter({:json_contains, "missing_json_filter", %{"zone" => "A"}})
    end

    assert_raise ArgumentError, ~r/missing_json_sort/, fn ->
      selecto()
      |> Selecto.json_order_by(
        {:json_extract_text, "missing_json_sort", "$.warehouse.zone", :asc}
      )
    end
  end

  test "array helper builders validate fields eagerly" do
    query =
      selecto()
      |> Selecto.array_manipulate({:array_to_string, "tags", ", ", as: "tag_list"})

    assert [array_spec] = query.set.array_operations
    assert array_spec.column == "tags"

    assert_raise ArgumentError, ~r/missing_array_filter/, fn ->
      selecto()
      |> Selecto.array_filter({:array_contains, "missing_array_filter", ["featured"]})
    end

    assert_raise ArgumentError, ~r/missing_array_column/, fn ->
      selecto()
      |> Selecto.array_manipulate(
        {:array_to_string, "missing_array_column", ", ", as: "tag_list"}
      )
    end

    assert_raise ArgumentError, ~r/missing_array_sort/, fn ->
      selecto()
      |> Selecto.array_manipulate(
        {:array_agg, "status", [order_by: [{"missing_array_sort", :asc}], as: "statuses"]}
      )
    end
  end
end
