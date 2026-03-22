defmodule Selecto.ExprTest do
  use ExUnit.Case, async: true

  alias Selecto.Expr, as: X

  defp selecto do
    domain = %{
      name: "Expr test",
      source: %{
        source_table: "products",
        primary_key: :id,
        fields: [:id, :name, :nickname, :status, :active, :price],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          nickname: %{type: :string},
          status: %{type: :string},
          active: %{type: :boolean},
          price: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    Selecto.configure(domain, :mock_connection)
  end

  test "builds filter helpers and compact boolean groups" do
    assert X.eq("status", "active") == {"status", "active"}
    assert X.neq("status", "archived") == {"status", {:ne, "archived"}}
    assert X.gte("price", 100) == {"price", {:gte, 100}}
    assert X.starts_with("name", "Ch") == {"name", {:like, "Ch%"}}
    assert X.when_present(nil, &X.eq("name", &1)) == nil

    assert X.compact_and([
             X.eq("status", "active"),
             nil,
             X.when_present("", &X.ilike("name", "%#{&1}%")),
             X.gte("price", 100)
           ]) == {:and, [{"status", "active"}, {"price", {:gte, 100}}]}
  end

  test "builds selector helpers with aliases and case literals" do
    assert X.field("name") == {:field, "name"}
    assert X.lit("Open") == {:literal, "Open"}
    assert X.count("*") == {:count, "*"}
    assert X.as(X.count("*"), "total") == {:field, {:count, "*"}, "total"}

    assert X.case_when(
             [
               {X.eq("status", "active"), "Open"},
               {X.eq("status", "archived"), "Closed"}
             ],
             "Other"
           ) ==
             {:case,
              [
                {{"status", "active"}, {:literal, "Open"}},
                {{"status", "archived"}, {:literal, "Closed"}}
              ], {:literal, "Other"}}
  end

  test "normalizes helper tuples into Selecto AST" do
    assert X.normalize({:eq, "status", "active"}) == {"status", "active"}
    assert X.normalize({:as, {:count, "*"}, "total"}) == {:field, {:count, "*"}, "total"}
    assert X.normalize({:desc, "price"}) == {"price", :desc}
    assert X.normalize({:asc_nulls_last, "price"}) == {"price", :asc_nulls_last}

    assert X.normalize({:window, {:lag, "price", 1}, over: [partition_by: ["status"]]}) ==
             {:window, {:lag, "price", 1}, over: [partition_by: ["status"]]}

    assert X.normalize({:and, [{:eq, "status", "active"}, {:gte, "price", 100}]}) ==
             {:and, [{"status", "active"}, {"price", {:gte, 100}}]}
  end

  test "builds window and json helper tuples" do
    assert X.window(:row_number, [],
             over: [partition_by: ["status"], order_by: [X.desc("price")]]
           ) ==
             {:window, {:row_number},
              over: [partition_by: ["status"], order_by: [{"price", :desc}]]}

    assert X.json_extract_text("metadata", "$.warehouse.zone", as: :warehouse_zone) ==
             {:json_extract_text, "metadata", "$.warehouse.zone", as: "warehouse_zone"}

    assert X.json_extract("metadata", "$.priority", :desc) ==
             {:json_extract, "metadata", "$.priority", :desc}
  end

  test "query entry points normalize helper-shaped inputs" do
    query =
      selecto()
      |> Selecto.select({:as, {:count, "*"}, "total"})
      |> Selecto.filter({:and, [{:eq, "active", true}, {:gte, "price", 100}]})
      |> Selecto.order_by({:desc_nulls_last, "price"})
      |> Selecto.group_by(X.rollup([{:field, "status"}]))

    assert query.set.selected == [{:field, {:count, "*"}, "total"}]
    assert query.set.filtered == [{:and, [{"active", true}, {"price", {:gte, 100}}]}]
    assert query.set.order_by == [{"price", :desc_nulls_last}]
    assert query.set.group_by == [rollup: [{:field, "status"}]]
  end

  test "pipeline helpers compose filters and selects" do
    query =
      selecto()
      |> X.merge_where([
        X.eq("active", true),
        X.when_present(nil, &X.eq("status", &1)),
        X.ilike("name", "%chair%")
      ])
      |> X.append_select([
        X.field("name"),
        X.as(X.coalesce([X.field("nickname"), X.field("name")]), "display_name")
      ])

    assert query.set.filtered ==
             [{:and, [{"active", true}, {"name", {:ilike, "%chair%"}}]}]

    assert query.set.selected == [
             {:field, "name"},
             {:field, {:coalesce, [{:field, "nickname"}, {:field, "name"}]}, "display_name"}
           ]

    {sql, params} = Selecto.to_sql(query)

    assert sql =~ ~r/coalesce/i
    assert sql =~ ~r/ilike/i
    assert true in params
    assert "%chair%" in params
  end
end
