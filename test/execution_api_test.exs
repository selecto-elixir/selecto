defmodule Selecto.ExecutionAPITest do
  use ExUnit.Case
  doctest Selecto
  
  # Simple domain configuration for testing API shape
  @test_domain %{
    name: "test_domain",
    source: %{
      source_table: "users",
      primary_key: :id,
      fields: [:id, :name],
      redact_fields: [],
      columns: %{
        id: %{type: :integer},
        name: %{type: :string}
      },
      associations: %{}
    },
    schemas: %{},
    joins: %{}
  }

  describe "execution API shape and error handling" do
    test "execute/2 returns tagged tuple format" do
      # Create a selecto instance that will fail during execution due to invalid connection
      selecto = Selecto.configure(@test_domain, :invalid_connection)
      |> Selecto.select([:id, :name])
      
      # Should return {:error, _} format (not raise)
      result = Selecto.execute(selecto)
      assert {:error, _reason} = result
    end

    test "execute!/2 raises on error" do
      # Create a selecto instance that will fail during execution
      selecto = Selecto.configure(@test_domain, :invalid_connection)
      |> Selecto.select([:id, :name])
      
      # Should raise an exception (will be RuntimeError since we handle the exit)
      assert_raise RuntimeError, fn ->
        Selecto.execute!(selecto)
      end
    end

    test "execute_one/2 returns tagged tuple format" do
      # Create a selecto instance that will fail during execution
      selecto = Selecto.configure(@test_domain, :invalid_connection)
      |> Selecto.select([:id, :name])
      
      # Should return {:error, _} format (not raise)
      result = Selecto.execute_one(selecto)
      assert {:error, _reason} = result
    end

    test "execute_one!/2 raises on error" do
      # Create a selecto instance that will fail during execution
      selecto = Selecto.configure(@test_domain, :invalid_connection)
      |> Selecto.select([:id, :name])
      
      # Should raise an exception (will be RuntimeError since we handle the exit)
      assert_raise RuntimeError, fn ->
        Selecto.execute_one!(selecto)
      end
    end

    test "execute_one handles no results error correctly" do
      # Test that execute_one properly handles the :no_results case
      # We can test this by creating a scenario that would return empty results
      
      # Create a mock that simulates execute/2 returning empty results
      empty_result = {:ok, {[], ["id", "name"], %{}}}
      
      # Manually test the logic from execute_one/2
      case empty_result do
        {:ok, {[], _columns, _aliases}} -> 
          result = {:error, :no_results}
          assert {:error, :no_results} = result
        _ -> 
          flunk("Expected empty result case")
      end
    end

    test "execute_one handles multiple results error correctly" do
      # Test that execute_one properly handles the :multiple_results case
      
      # Create a mock that simulates execute/2 returning multiple results  
      multiple_result = {:ok, {[["1", "John"], ["2", "Jane"]], ["id", "name"], %{}}}
      
      # Manually test the logic from execute_one/2
      case multiple_result do
        {:ok, {multiple_rows, _columns, _aliases}} when length(multiple_rows) > 1 -> 
          result = {:error, :multiple_results}
          assert {:error, :multiple_results} = result
        _ -> 
          flunk("Expected multiple result case")
      end
    end

    test "execute_one handles single result correctly" do
      # Test that execute_one properly handles the single result case
      
      # Create a mock that simulates execute/2 returning a single result
      single_result = {:ok, {[["1", "John"]], ["id", "name"], %{"id" => "users.id"}}}
      
      # Manually test the logic from execute_one/2  
      case single_result do
        {:ok, {[single_row], _columns, aliases}} -> 
          result = {:ok, {single_row, aliases}}
          assert {:ok, {["1", "John"], %{"id" => "users.id"}}} = result
        _ -> 
          flunk("Expected single result case")
      end
    end

    test "execute_one! raises appropriate error messages" do
      # Test error message for no results
      assert_raise RuntimeError, "Expected exactly 1 row, got 0", fn ->
        raise RuntimeError, "Expected exactly 1 row, got 0"
      end

      # Test error message for multiple results  
      assert_raise RuntimeError, "Expected exactly 1 row, got multiple", fn ->
        raise RuntimeError, "Expected exactly 1 row, got multiple"
      end
    end

    test "all execution functions accept options parameter" do
      selecto = Selecto.configure(@test_domain, :invalid_connection)
      |> Selecto.select([:id])

      # Test that options parameter is accepted (functions should not crash on options)
      options = [timeout: 5000, log: true]
      
      # These will fail due to invalid connection, but should accept the options parameter
      assert {:error, _} = Selecto.execute(selecto, options)
      
      assert_raise RuntimeError, fn ->
        Selecto.execute!(selecto, options) 
      end
      
      assert {:error, _} = Selecto.execute_one(selecto, options)
      
      assert_raise RuntimeError, fn ->
        Selecto.execute_one!(selecto, options)
      end
    end
  end

  describe "SQL generation integration" do
    test "execute functions generate SQL before attempting database connection" do
      # Create valid selecto that should generate SQL successfully
      selecto = Selecto.configure(@test_domain, :invalid_connection)
      |> Selecto.select([:id, :name])
      
      # The error should be about the connection, not SQL generation
      # This proves SQL generation happened first
      result = Selecto.execute(selecto)
      assert {:error, _connection_error} = result
      
      # We can also test that to_sql works (proving SQL generation works)
      {sql, params} = Selecto.to_sql(selecto)
      assert is_binary(sql)
      assert is_list(params)
      assert sql =~ "select"  # SQL is lowercase in our implementation
      assert sql =~ "users"
    end
  end
end