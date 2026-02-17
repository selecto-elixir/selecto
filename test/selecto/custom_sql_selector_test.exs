defmodule Selecto.CustomSqlSelectorTest do
  use ExUnit.Case, async: true

  alias Selecto.Builder.Sql.Select

  describe "custom SQL selector field validation" do
    test "accepts CTE-qualified fields when CTE columns are declared" do
      selecto = %{
        config: %{
          columns: %{"id" => %{}, "name" => %{}},
          joins: %{}
        },
        set: %{
          ctes: [
            %{name: "active_customers", columns: ["customer_id", "first_name"]}
          ]
        }
      }

      field_mappings = %{"cte_id" => "active_customers.customer_id"}
      sql_template = "COALESCE({{cte_id}}, 0)"

      {result_iodata, join, params} =
        Select.prep_selector(selecto, {:custom_sql, sql_template, field_mappings})

      assert is_list(result_iodata)
      assert join == :selecto_root
      assert params == []
      assert IO.iodata_to_binary(result_iodata) =~ "active_customers.customer_id"
    end
  end
end
