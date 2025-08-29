defmodule Selecto.Advanced.ValuesClauseTest do
  use ExUnit.Case, async: true
  
  alias Selecto.Advanced.ValuesClause
  alias Selecto.Advanced.ValuesClause.{Spec, ValidationError}
  
  describe "VALUES clause specification creation" do
    test "creates spec with list of lists data" do
      data = [
        ["PG", "Family Friendly", 1],
        ["PG-13", "Teen", 2],
        ["R", "Adult", 3]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["rating_code", "description", "sort_order"],
        as: "rating_lookup"
      )
      
      assert %Spec{
        data: ^data,
        columns: ["rating_code", "description", "sort_order"],
        alias: "rating_lookup",
        data_type: :list_of_lists,
        validated: true
      } = spec
      
      assert is_binary(spec.id)
      assert String.starts_with?(spec.id, "values_rating_lookup_")
    end
    
    test "creates spec with list of maps data" do
      data = [
        %{month: 1, name: "January", days: 31},
        %{month: 2, name: "February", days: 28},
        %{month: 3, name: "March", days: 31}
      ]
      
      spec = ValuesClause.create_values_clause(data, as: "months")
      
      assert %Spec{
        data: ^data,
        alias: "months",
        data_type: :list_of_maps,
        validated: true
      } = spec
      
      # Columns should be inferred from map keys (sorted)
      assert spec.columns == ["days", "month", "name"]
    end
    
    test "uses default alias when none provided" do
      data = [["A", 1], ["B", 2]]
      
      spec = ValuesClause.create_values_clause(data, columns: ["letter", "number"])
      
      assert spec.alias == "values_table"
    end
    
    test "generates default column names for list of lists without explicit columns" do
      data = [["A", 1, true], ["B", 2, false]]
      
      spec = ValuesClause.create_values_clause(data)
      
      assert spec.columns == ["column1", "column2", "column3"]
    end
  end
  
  describe "data validation" do
    test "validates empty data" do
      assert_raise ValidationError, ~r/empty data/, fn ->
        ValuesClause.create_values_clause([])
      end
    end
    
    test "validates inconsistent list lengths" do
      data = [
        ["PG", "Family", 1],
        ["PG-13", "Teen"]  # Missing third column
      ]
      
      assert_raise ValidationError, ~r/Row 2 has 2 columns, expected 3/, fn ->
        ValuesClause.create_values_clause(data)
      end
    end
    
    test "validates inconsistent map keys" do
      data = [
        %{code: "PG", desc: "Family"},
        %{code: "R", description: "Adult"}  # Different key name
      ]
      
      assert_raise ValidationError, ~r/different keys than first row/, fn ->
        ValuesClause.create_values_clause(data)
      end
    end
    
    test "validates column count mismatch for lists" do
      data = [["A", 1], ["B", 2]]
      
      assert_raise ValidationError, ~r/explicit columns.*doesn't match data columns/, fn ->
        ValuesClause.create_values_clause(data, columns: ["letter"])  # Only 1 column for 2-column data
      end
    end
    
    test "validates column mismatch for maps" do
      data = [%{a: 1, b: 2}, %{a: 3, b: 4}]
      
      assert_raise ValidationError, "Explicit columns don't match map keys", fn ->
        ValuesClause.create_values_clause(data, columns: ["a", "c"])  # 'c' not in map
      end
    end
  end
  
  describe "type inference" do
    test "infers types from list data" do
      data = [
        ["string", 42, 3.14, true, nil],
        ["another", 99, 2.71, false, nil]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["text_col", "int_col", "decimal_col", "bool_col", "null_col"]
      )
      
      assert spec.column_types["text_col"] == :string
      assert spec.column_types["int_col"] == :integer
      assert spec.column_types["decimal_col"] == :decimal
      assert spec.column_types["bool_col"] == :boolean
      assert spec.column_types["null_col"] == :unknown
    end
    
    test "infers types from map data" do
      data = [
        %{name: "John", age: 30, score: 95.5, active: true},
        %{name: "Jane", age: 25, score: 88.2, active: false}
      ]
      
      spec = ValuesClause.create_values_clause(data)
      
      assert spec.column_types["name"] == :string
      assert spec.column_types["age"] == :integer
      assert spec.column_types["score"] == :decimal
      assert spec.column_types["active"] == :boolean
    end
    
    test "handles date/time types" do
      data = [
        [~D[2023-01-01], ~U[2023-01-01 12:00:00Z], ~N[2023-01-01 12:00:00]]
      ]
      
      spec = ValuesClause.create_values_clause(data, 
        columns: ["date_col", "datetime_col", "naive_datetime_col"]
      )
      
      assert spec.column_types["date_col"] == :date
      assert spec.column_types["datetime_col"] == :utc_datetime
      assert spec.column_types["naive_datetime_col"] == :naive_datetime
    end
  end
  
  describe "mixed data type validation" do
    test "rejects mixed lists and maps" do
      data = [
        ["A", 1],
        %{letter: "B", number: 2}
      ]
      
      assert_raise ValidationError, ~r/Row 2 has 0 columns, expected 2/, fn ->
        ValuesClause.create_values_clause(data)
      end
    end
    
    test "rejects non-list, non-map rows" do
      data = [
        ["A", 1],
        "invalid_row"
      ]
      
      assert_raise ValidationError, ~r/Row 2 has 0 columns, expected 2/, fn ->
        ValuesClause.create_values_clause(data)
      end
    end
  end
  
  describe "edge cases" do
    test "handles single row of data" do
      data = [["single", "row"]]
      
      spec = ValuesClause.create_values_clause(data, columns: ["col1", "col2"])
      
      assert spec.data == data
      assert spec.columns == ["col1", "col2"]
      assert spec.validated == true
    end
    
    test "handles large column counts" do
      # Create a row with 20 columns
      row_data = Enum.map(1..20, fn i -> "value#{i}" end)
      data = [row_data]
      
      spec = ValuesClause.create_values_clause(data)
      
      assert length(spec.columns) == 20
      assert spec.columns == Enum.map(1..20, fn i -> "column#{i}" end)
    end
    
    test "handles nil values correctly" do
      data = [
        [nil, "text", nil],
        ["text", nil, "more"]
      ]
      
      spec = ValuesClause.create_values_clause(data, columns: ["a", "b", "c"])
      
      # Type inference should work with non-nil values
      assert spec.column_types["b"] == :string
      assert spec.column_types["c"] == :string
    end
    
    test "handles empty strings" do
      data = [
        ["", "text"],
        ["text", ""]
      ]
      
      spec = ValuesClause.create_values_clause(data, columns: ["a", "b"])
      
      assert spec.column_types["a"] == :string
      assert spec.column_types["b"] == :string
    end
  end
  
  describe "column name handling" do
    test "converts atom column names to strings" do
      data = [["A", 1], ["B", 2]]
      
      spec = ValuesClause.create_values_clause(data, columns: [:letter, :number])
      
      assert spec.columns == ["letter", "number"]
    end
    
    test "handles string column names" do
      data = [["A", 1], ["B", 2]]
      
      spec = ValuesClause.create_values_clause(data, columns: ["letter", "number"])
      
      assert spec.columns == ["letter", "number"]
    end
    
    test "sorts map keys consistently" do
      # Use keys that would sort differently as atoms vs strings
      data = [
        %{zebra: 1, apple: 2, banana: 3},
        %{zebra: 4, apple: 5, banana: 6}
      ]
      
      spec = ValuesClause.create_values_clause(data)
      
      # Should be sorted alphabetically as strings
      assert spec.columns == ["apple", "banana", "zebra"]
    end
  end
  
  describe "error details" do
    test "provides helpful error details for inconsistent columns" do
      data = [
        ["A", 1, true],
        ["B", 2]  # Missing column
      ]
      
      error = catch_error(ValuesClause.create_values_clause(data))
      
      assert %ValidationError{
        type: :inconsistent_columns,
        details: %{
          expected_length: 3,
          actual_length: 2,
          row_index: 2
        }
      } = error
    end
    
    test "provides available fields for map key mismatches" do
      data = [
        %{name: "John", age: 30},
        %{name: "Jane", years: 25}  # Different key
      ]
      
      error = catch_error(ValuesClause.create_values_clause(data))
      
      assert %ValidationError{
        type: :inconsistent_columns,
        details: %{
          expected_keys: ["age", "name"],
          actual_keys: ["name", "years"],
          row_index: 2
        }
      } = error
    end
  end
end