defmodule Selecto.SQLFunctionsTest do
  use ExUnit.Case, async: true
  
  alias Selecto.SQL.Functions
  
  # Test domain for function testing
  @test_domain %{
    name: "SQL Functions Test Domain",
    source: %{
      source_table: "products",
      primary_key: :id,
      fields: [:id, :name, :description, :price, :category, :created_at, :tags],
      redact_fields: [],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string},
        description: %{type: :string},
        price: %{type: :decimal},
        category: %{type: :string},
        created_at: %{type: :utc_datetime},
        tags: %{type: {:array, :string}}
      },
      associations: %{}
    },
    schemas: %{},
    default_selected: ["id", "name"],
    joins: %{},
    filters: %{}
  }
  
  setup do
    selecto = Selecto.configure(@test_domain, :mock_connection)
    {:ok, selecto: selecto}
  end
  
  describe "string functions" do
    test "substr with start and length", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:substr, "description", 1, 50})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "substr(\"selecto_root\".\"description\", 1, 50)"
    end
    
    test "substr with start only", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:substr, "description", 5})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "substr(\"selecto_root\".\"description\", 5)"
    end
    
    test "trim function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:trim, "name"})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "trim(\"selecto_root\".\"name\")"
    end
    
    test "upper and lower functions", %{selecto: selecto} do
      upper_result = Functions.prep_advanced_selector(selecto, {:upper, "category"})
      lower_result = Functions.prep_advanced_selector(selecto, {:lower, "category"})
      
      assert {upper_iodata, :selecto_root, []} = upper_result
      assert {lower_iodata, :selecto_root, []} = lower_result
      
      upper_sql = IO.iodata_to_binary(upper_iodata)
      lower_sql = IO.iodata_to_binary(lower_iodata)
      
      assert upper_sql == "upper(\"selecto_root\".\"category\")"
      assert lower_sql == "lower(\"selecto_root\".\"category\")"
    end
    
    test "length function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:length, "name"})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "length(\"selecto_root\".\"name\")"
    end
    
    test "replace function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:replace, "description", {:literal, "old"}, {:literal, "new"}})
      assert {iodata, :selecto_root, params} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql =~ "replace("
      assert sql =~ "\"selecto_root\".\"description\""
      assert "old" in params
      assert "new" in params
    end
  end
  
  describe "mathematical functions" do
    test "abs function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:abs, "price"})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "abs(\"selecto_root\".\"price\")"
    end
    
    test "round functions", %{selecto: selecto} do
      round_result = Functions.prep_advanced_selector(selecto, {:round, "price"})
      round_precision_result = Functions.prep_advanced_selector(selecto, {:round, "price", 2})
      
      assert {round_iodata, :selecto_root, []} = round_result
      assert {round_prec_iodata, :selecto_root, []} = round_precision_result
      
      round_sql = IO.iodata_to_binary(round_iodata)
      round_prec_sql = IO.iodata_to_binary(round_prec_iodata)
      
      assert round_sql == "round(\"selecto_root\".\"price\")"
      assert round_prec_sql == "round(\"selecto_root\".\"price\", 2)"
    end
    
    test "power function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:power, "price", {:literal, 2}})
      assert {iodata, :selecto_root, params} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql =~ "power("
      assert sql =~ "\"selecto_root\".\"price\""
      assert 2 in params
    end
    
    test "sqrt function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:sqrt, "price"})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "sqrt(\"selecto_root\".\"price\")"
    end
    
    test "random function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:random})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "random()"
    end
  end
  
  describe "date/time functions" do
    test "now function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:now})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "now()"
    end
    
    test "date_trunc function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:date_trunc, {:literal, "month"}, "created_at"})
      assert {iodata, :selecto_root, params} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql =~ "date_trunc("
      assert sql =~ "\"selecto_root\".\"created_at\""
      assert "month" in params
    end
    
    test "interval functions", %{selecto: selecto} do
      string_result = Functions.prep_advanced_selector(selecto, {:interval, "1 day"})
      tuple_result = Functions.prep_advanced_selector(selecto, {:interval, {7, "days"}})
      
      assert {string_iodata, :selecto_root, []} = string_result
      assert {tuple_iodata, :selecto_root, []} = tuple_result
      
      string_sql = IO.iodata_to_binary(string_iodata)
      tuple_sql = IO.iodata_to_binary(tuple_iodata)
      
      assert string_sql == "interval '1 day'"
      assert tuple_sql == "interval '7 days'"
    end
    
    test "age function", %{selecto: selecto} do
      single_result = Functions.prep_advanced_selector(selecto, {:age, "created_at"})
      double_result = Functions.prep_advanced_selector(selecto, {:age, "created_at", {:now}})
      
      assert {single_iodata, :selecto_root, []} = single_result
      assert {double_iodata, joins, []} = double_result
      
      single_sql = IO.iodata_to_binary(single_iodata)
      double_sql = IO.iodata_to_binary(double_iodata)
      
      assert single_sql == "age(\"selecto_root\".\"created_at\")"
      assert double_sql =~ "age("
      assert double_sql =~ "\"selecto_root\".\"created_at\""
      assert double_sql =~ "now()"
    end
  end
  
  describe "array functions" do
    test "array_agg function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:array_agg, "category"})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "array_agg(\"selecto_root\".\"category\")"
    end
    
    test "array_length function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:array_length, "tags"})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "array_length(\"selecto_root\".\"tags\", 1)"
    end
    
    test "array_to_string function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:array_to_string, "tags", {:literal, ", "}})
      assert {iodata, :selecto_root, params} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql =~ "array_to_string("
      assert sql =~ "\"selecto_root\".\"tags\""
      assert ", " in params
    end
    
    test "unnest function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:unnest, "tags"})
      assert {iodata, :selecto_root, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "unnest(\"selecto_root\".\"tags\")"
    end
  end
  
  describe "window functions" do
    test "row_number function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:window, {:row_number}, over: []})
      assert {iodata, [], []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql == "row_number() over ()"
    end
    
    test "window function with partition by", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {
        :window, 
        {:row_number}, 
        over: [partition_by: ["category"]]
      })
      assert {iodata, joins, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql =~ "row_number() over ("
      assert sql =~ "partition by"
      assert sql =~ "\"selecto_root\".\"category\""
    end
    
    test "window function with order by", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {
        :window, 
        {:rank}, 
        over: [order_by: ["price"]]
      })
      assert {iodata, joins, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql =~ "rank() over ("
      assert sql =~ "order by"
      assert sql =~ "\"selecto_root\".\"price\""
    end
    
    test "lag and lead functions", %{selecto: selecto} do
      lag_result = Functions.prep_advanced_selector(selecto, {
        :window, 
        {:lag, "price"}, 
        over: [partition_by: ["category"], order_by: ["created_at"]]
      })
      
      lead_result = Functions.prep_advanced_selector(selecto, {
        :window, 
        {:lead, "price", 2}, 
        over: [partition_by: ["category"], order_by: ["created_at"]]
      })
      
      assert {lag_iodata, _, []} = lag_result
      assert {lead_iodata, _, []} = lead_result
      
      lag_sql = IO.iodata_to_binary(lag_iodata)
      lead_sql = IO.iodata_to_binary(lead_iodata)
      
      assert lag_sql =~ "lag("
      assert lag_sql =~ "\"selecto_root\".\"price\""
      assert lag_sql =~ "partition by"
      assert lag_sql =~ "order by"
      
      assert lead_sql =~ "lead("
      assert lead_sql =~ "\"selecto_root\".\"price\", 2"
    end
    
    test "ntile function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {
        :window, 
        {:ntile, 4}, 
        over: [order_by: ["price"]]
      })
      assert {iodata, _, []} = result
      sql = IO.iodata_to_binary(iodata)
      assert sql =~ "ntile(4) over ("
      assert sql =~ "order by"
    end
  end
  
  describe "conditional functions" do
    test "iif function", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {
        :iif, 
        {"price", :gt, {:literal, 100}}, 
        {:literal, "expensive"}, 
        {:literal, "affordable"}
      })
      
      assert {iodata, joins, params} = result
      sql = IO.iodata_to_binary(iodata)
      
      assert sql =~ "case when"
      assert sql =~ "\"selecto_root\".\"price\""
      assert sql =~ "then"
      assert sql =~ "else"
      assert sql =~ "end"
      assert "expensive" in params
      assert "affordable" in params
      assert 100 in params
    end
    
    test "decode function", %{selecto: selecto} do
      mappings = [
        {{"category", :eq, {:literal, "electronics"}}, {:literal, "tech"}},
        {{"category", :eq, {:literal, "books"}}, {:literal, "literature"}}
      ]
      
      result = Functions.prep_advanced_selector(selecto, {:decode, "category", mappings})
      assert {iodata, joins, params} = result
      sql = IO.iodata_to_binary(iodata)
      
      assert sql =~ "decode("
      assert sql =~ "\"selecto_root\".\"category\""
      # Parameters should include the mapping values
      assert "electronics" in params or "tech" in params
    end
  end
  
  describe "integration with existing functions" do
    test "unsupported selector returns nil", %{selecto: selecto} do
      result = Functions.prep_advanced_selector(selecto, {:unknown_function, "field"})
      assert result == nil
    end
    
    test "existing functions still work", %{selecto: selecto} do
      # Test that existing functions in the main SELECT module still work
      # This would require integration testing with the full Selecto.configure flow
      
      domain = %{
        source: %{
          source_table: "test_table",
          primary_key: :id,
          fields: [:id, :name, :value],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}, 
            value: %{type: :decimal}
          }
        },
        schemas: %{}
      }
      
      selecto = Selecto.configure(domain, :mock_connection)
      |> Selecto.select([
        "name",
        {:coalesce, ["name", {:literal, "unknown"}]},  # Existing function
        {:upper, "name"}  # New function
      ])
      
      # Generate SQL to verify both work
      {sql, aliases, params} = Selecto.gen_sql(selecto, [])
      
      assert String.contains?(sql, "coalesce(")  # Existing function
      assert String.contains?(sql, "upper(")     # New function
      assert is_list(aliases)
      assert is_list(params)
    end
  end
end