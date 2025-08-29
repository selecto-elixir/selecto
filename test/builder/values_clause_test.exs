defmodule Selecto.Builder.ValuesClauseTest do
  use ExUnit.Case, async: true
  
  alias Selecto.Advanced.ValuesClause
  alias Selecto.Builder.ValuesClause, as: Builder
  
  describe "basic VALUES SQL generation" do
    test "generates SQL for list of lists data" do
      data = [
        ["PG", "Family Friendly", 1],
        ["PG-13", "Teen", 2],
        ["R", "Adult", 3]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["rating_code", "description", "sort_order"],
        as: "rating_lookup"
      )
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES ('PG', 'Family Friendly', 1), ('PG-13', 'Teen', 2), ('R', 'Adult', 3) AS rating_lookup ("rating_code", "description", "sort_order")]
      assert sql_string == expected
    end
    
    test "generates SQL for list of maps data" do
      data = [
        %{month: 1, name: "January", days: 31},
        %{month: 2, name: "February", days: 28}
      ]
      
      spec = ValuesClause.create_values_clause(data, as: "months")
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      # Maps are ordered by sorted keys: days, month, name
      expected = ~s[VALUES (31, 1, 'January'), (28, 2, 'February') AS months ("days", "month", "name")]
      assert sql_string == expected
    end
    
    test "handles NULL values correctly" do
      data = [
        ["A", nil, 1],
        [nil, "B", 2]
      ]
      
      spec = ValuesClause.create_values_clause(data, columns: ["col1", "col2", "col3"])
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES ('A', NULL, 1), (NULL, 'B', 2) AS values_table ("col1", "col2", "col3")]
      assert sql_string == expected
    end
    
    test "handles different data types" do
      data = [
        ["text", 42, 3.14, true, false]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["text_col", "int_col", "decimal_col", "bool_true", "bool_false"]
      )
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES ('text', 42, 3.14, TRUE, FALSE) AS values_table ("text_col", "int_col", "decimal_col", "bool_true", "bool_false")]
      assert sql_string == expected
    end
  end
  
  describe "CTE VALUES SQL generation" do
    test "generates CTE SQL for VALUES clause" do
      data = [
        ["PG", "Family"],
        ["R", "Adult"]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["code", "description"],
        as: "ratings"
      )
      
      cte_sql_iodata = Builder.build_values_cte(spec)
      cte_sql_string = IO.iodata_to_binary(cte_sql_iodata)
      
      expected = ~s[ratings ("code", "description") AS (VALUES ('PG', 'Family'), ('R', 'Adult'))]
      assert cte_sql_string == expected
    end
  end
  
  describe "parameterized VALUES SQL generation" do
    test "generates parameterized SQL for list of lists" do
      data = [
        ["PG", "Family", 1],
        ["R", "Adult", 3]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["code", "desc", "order"],
        as: "ratings"
      )
      
      {sql_iodata, params} = Builder.build_values_clause_with_params(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected_sql = ~s[VALUES ($1, $2, $3), ($4, $5, $6) AS ratings ("code", "desc", "order")]
      expected_params = ["PG", "Family", 1, "R", "Adult", 3]
      
      assert sql_string == expected_sql
      assert params == expected_params
    end
    
    test "generates parameterized SQL for list of maps" do
      data = [
        %{name: "Alice", age: 30},
        %{name: "Bob", age: 25}
      ]
      
      spec = ValuesClause.create_values_clause(data, as: "people")
      
      {sql_iodata, params} = Builder.build_values_clause_with_params(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      # Map keys are sorted: age, name
      expected_sql = ~s[VALUES ($1, $2), ($3, $4) AS people ("age", "name")]
      expected_params = [30, "Alice", 25, "Bob"]
      
      assert sql_string == expected_sql
      assert params == expected_params
    end
  end
  
  describe "data type formatting" do
    test "formats string values with proper escaping" do
      data = [
        ["O'Reilly", "He said \"Hello\""],
        ["Text with, commas", "Text with\nnewlines"]
      ]
      
      spec = ValuesClause.create_values_clause(data, columns: ["col1", "col2"])
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      # Single quotes should be escaped, double quotes and other chars should be preserved
      expected = ~s[VALUES ('O''Reilly', 'He said "Hello"'), ('Text with, commas', 'Text with\nnewlines') AS values_table ("col1", "col2")]
      assert sql_string == expected
    end
    
    test "formats date and time values" do
      data = [
        [~D[2023-01-01], ~U[2023-01-01 12:00:00Z], ~N[2023-01-01 12:00:00]]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["date_col", "datetime_col", "naive_datetime_col"]
      )
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES ('2023-01-01', '2023-01-01T12:00:00Z', '2023-01-01T12:00:00') AS values_table ("date_col", "datetime_col", "naive_datetime_col")]
      assert sql_string == expected
    end
    
    test "formats numeric values without quotes" do
      data = [
        [42, 3.14159, -10, 0.0]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["int", "float", "negative", "zero"]
      )
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES (42, 3.14159, -10, 0.0) AS values_table ("int", "float", "negative", "zero")]
      assert sql_string == expected
    end
  end
  
  describe "column identifier quoting" do
    test "quotes column names to handle reserved words" do
      data = [["value1", "value2"]]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["select", "from"],  # SQL reserved words
        as: "test_table"
      )
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES ('value1', 'value2') AS test_table ("select", "from")]
      assert sql_string == expected
    end
    
    test "quotes column names with special characters" do
      data = [["value1", "value2"]]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["column with spaces", "column-with-dashes"],
        as: "test_table"
      )
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES ('value1', 'value2') AS test_table ("column with spaces", "column-with-dashes")]
      assert sql_string == expected
    end
  end
  
  describe "error handling" do
    test "raises error for unvalidated specs" do
      # Create an unvalidated spec manually
      spec = %ValuesClause.Spec{
        id: "test_id",
        data: [["A", 1]],
        columns: ["col1", "col2"],
        alias: "test",
        data_type: :list_of_lists,
        validated: false
      }
      
      assert_raise ArgumentError, ~r/must be validated before SQL generation/, fn ->
        Builder.build_values_clause(spec)
      end
    end
    
    test "raises error for unvalidated CTE specs" do
      spec = %ValuesClause.Spec{
        id: "test_id",
        data: [["A", 1]],
        columns: ["col1", "col2"],
        alias: "test",
        data_type: :list_of_lists,
        validated: false
      }
      
      assert_raise ArgumentError, ~r/must be validated before CTE generation/, fn ->
        Builder.build_values_cte(spec)
      end
    end
    
    test "raises error for unvalidated parameterized specs" do
      spec = %ValuesClause.Spec{
        id: "test_id",
        data: [["A", 1]],
        columns: ["col1", "col2"],
        alias: "test",
        data_type: :list_of_lists,
        validated: false
      }
      
      assert_raise ArgumentError, ~r/must be validated before parameterized SQL generation/, fn ->
        Builder.build_values_clause_with_params(spec)
      end
    end
  end
  
  describe "edge cases" do
    test "handles single row" do
      data = [["single"]]
      
      spec = ValuesClause.create_values_clause(data, columns: ["col"])
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected = ~s[VALUES ('single') AS values_table ("col")]
      assert sql_string == expected
    end
    
    test "handles many columns" do
      row_data = Enum.map(1..10, fn i -> "value#{i}" end)
      data = [row_data]
      
      spec = ValuesClause.create_values_clause(data)
      
      sql_iodata = Builder.build_values_clause(spec)
      sql_string = IO.iodata_to_binary(sql_iodata)
      
      expected_values = Enum.map(1..10, fn i -> "'value#{i}'" end) |> Enum.join(", ")
      expected_columns = Enum.map(1..10, fn i -> "\"column#{i}\"" end) |> Enum.join(", ")
      expected = "VALUES (#{expected_values}) AS values_table (#{expected_columns})"
      
      assert sql_string == expected
    end
  end
end