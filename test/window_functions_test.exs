defmodule WindowFunctionsTest do
  use ExUnit.Case, async: true

  alias Selecto.Window
  alias Selecto.Window.{Spec, Frame}

  describe "Window function API" do
    setup do
      # Create a basic selecto struct for testing
      selecto = %Selecto{
        domain: %{},
        postgrex_opts: [],
        set: %{}
      }
      
      {:ok, selecto: selecto}
    end

    test "adds ROW_NUMBER window function", %{selecto: selecto} do
      result = Selecto.window_function(selecto, :row_number, [], 
        over: [partition_by: ["category"], order_by: ["created_at"]])
      
      window_functions = get_in(result.set, [:window_functions])
      assert length(window_functions) == 1
      
      [window_spec] = window_functions
      assert %Spec{
        function: :row_number,
        arguments: nil,
        partition_by: ["category"],
        order_by: [{"created_at", :asc}]
      } = window_spec
    end

    test "adds RANK window function with DESC order", %{selecto: selecto} do
      result = Selecto.window_function(selecto, :rank, [], 
        over: [partition_by: ["region"], order_by: [{"sales", :desc}]], as: "sales_rank")
      
      [window_spec] = get_in(result.set, [:window_functions])
      assert %Spec{
        function: :rank,
        order_by: [{"sales", :desc}],
        alias: "sales_rank"
      } = window_spec
    end

    test "adds SUM window function with arguments", %{selecto: selecto} do
      result = Selecto.window_function(selecto, :sum, ["amount"], 
        over: [partition_by: ["customer"], order_by: ["date"]], as: "running_total")
      
      [window_spec] = get_in(result.set, [:window_functions])
      assert %Spec{
        function: :sum,
        arguments: ["amount"],
        partition_by: ["customer"],
        alias: "running_total"
      } = window_spec
    end

    test "adds LAG window function with offset", %{selecto: selecto} do
      result = Selecto.window_function(selecto, :lag, ["sales", 2], 
        over: [partition_by: ["region"], order_by: ["month"]], as: "prev_sales")
      
      [window_spec] = get_in(result.set, [:window_functions])
      assert %Spec{
        function: :lag,
        arguments: ["sales", 2]
      } = window_spec
    end

    test "adds window function with frame specification", %{selecto: selecto} do
      result = Selecto.window_function(selecto, :avg, ["amount"], 
        over: [
          order_by: ["date"], 
          frame: {:rows, {:preceding, 3}, :current_row}
        ])
      
      [window_spec] = get_in(result.set, [:window_functions])
      assert %Spec{
        function: :avg,
        frame: %Frame{
          type: :rows,
          start: {:preceding, 3},
          end: :current_row
        }
      } = window_spec
    end

    test "supports multiple window functions", %{selecto: selecto} do
      result = selecto
        |> Selecto.window_function(:row_number, [], over: [order_by: ["date"]])
        |> Selecto.window_function(:sum, ["amount"], over: [order_by: ["date"]])
        |> Selecto.window_function(:lag, ["amount", 1], over: [order_by: ["date"]])
      
      window_functions = get_in(result.set, [:window_functions])
      assert length(window_functions) == 3
      
      functions = Enum.map(window_functions, & &1.function)
      assert functions == [:row_number, :sum, :lag]
    end
  end

  describe "Window function SQL generation" do
    setup do
      domain = SelectoTest.PagilaDomain.films_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "generates SQL for ROW_NUMBER", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "release_year"])
        |> Selecto.window_function(:row_number, [], 
             over: [partition_by: ["release_year"], order_by: ["title"]], 
             as: "row_num")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "ROW_NUMBER() OVER (PARTITION BY film.release_year ORDER BY film.title ASC) AS row_num"
    end

    test "generates SQL for SUM with running total", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.window_function(:sum, ["rental_rate"], 
             over: [order_by: ["title"]], 
             as: "running_total")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "SUM(film.rental_rate) OVER (ORDER BY film.title ASC) AS running_total"
    end

    test "generates SQL for LAG with offset", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.window_function(:lag, ["rental_rate", 2], 
             over: [order_by: ["release_year"]], 
             as: "prev_rate")
      
      {sql, params} = Selecto.to_sql(result)
      
      assert sql =~ "LAG(film.rental_rate, ?) OVER (ORDER BY film.release_year ASC) AS prev_rate"
      assert params == [2]
    end

    test "generates SQL for AVG with frame", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.window_function(:avg, ["rental_rate"], 
             over: [
               order_by: ["release_year"], 
               frame: {:rows, {:preceding, 2}, :current_row}
             ], 
             as: "moving_avg")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "AVG(film.rental_rate) OVER (ORDER BY film.release_year ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg"
    end

    test "generates SQL for NTILE", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.window_function(:ntile, [4], 
             over: [order_by: ["rental_rate"]], 
             as: "quartile")
      
      {sql, params} = Selecto.to_sql(result)
      
      assert sql =~ "NTILE(?) OVER (ORDER BY film.rental_rate ASC) AS quartile"
      assert params == [4]
    end

    test "generates SQL for DENSE_RANK with multiple partitions", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rating", "rental_rate"])
        |> Selecto.window_function(:dense_rank, [], 
             over: [partition_by: ["rating"], order_by: [{"rental_rate", :desc}]], 
             as: "rate_rank")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "DENSE_RANK() OVER (PARTITION BY film.rating ORDER BY film.rental_rate DESC) AS rate_rank"
    end

    test "generates SQL with multiple window functions", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.window_function(:row_number, [], over: [order_by: ["title"]], as: "row_num")
        |> Selecto.window_function(:sum, ["rental_rate"], over: [order_by: ["title"]], as: "running_sum")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "ROW_NUMBER() OVER (ORDER BY film.title ASC) AS row_num"
      assert sql =~ "SUM(film.rental_rate) OVER (ORDER BY film.title ASC) AS running_sum"
    end
  end

  describe "Window frame specifications" do
    test "parses ROWS frame correctly" do
      frame_spec = {:rows, {:preceding, 5}, {:following, 2}}
      frame = Window.parse_frame_public(frame_spec)
      
      assert %Frame{
        type: :rows,
        start: {:preceding, 5},
        end: {:following, 2}
      } = frame
    end

    test "parses RANGE frame correctly" do
      frame_spec = {:range, :unbounded_preceding, :current_row}
      frame = Window.parse_frame_public(frame_spec)
      
      assert %Frame{
        type: :range,
        start: :unbounded_preceding,
        end: :current_row
      } = frame
    end

    test "raises error for invalid frame" do
      assert_raise ArgumentError, fn ->
        Window.parse_frame_public({:invalid, :start, :end})
      end
    end

    test "raises error for invalid boundary" do
      assert_raise ArgumentError, fn ->
        Window.parse_frame_public({:rows, :invalid_boundary, :current_row})
      end
    end
  end

  describe "Window function edge cases" do
    setup do
      domain = SelectoTest.PagilaDomain.films_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "handles COUNT(*) window function", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title"])
        |> Selecto.window_function(:count, ["*"], over: [order_by: ["title"]], as: "running_count")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "COUNT(*) OVER (ORDER BY film.title ASC) AS running_count"
    end

    test "handles FIRST_VALUE and LAST_VALUE", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.window_function(:first_value, ["rental_rate"], 
             over: [partition_by: ["rating"], order_by: ["title"]], 
             as: "first_rate")
        |> Selecto.window_function(:last_value, ["rental_rate"], 
             over: [partition_by: ["rating"], order_by: ["title"]], 
             as: "last_rate")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "FIRST_VALUE(film.rental_rate) OVER (PARTITION BY film.rating ORDER BY film.title ASC) AS first_rate"
      assert sql =~ "LAST_VALUE(film.rental_rate) OVER (PARTITION BY film.rating ORDER BY film.title ASC) AS last_rate"
    end

    test "handles statistical window functions", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.window_function(:stddev, ["rental_rate"], 
             over: [partition_by: ["rating"]], 
             as: "rate_stddev")
        |> Selecto.window_function(:variance, ["rental_rate"], 
             over: [partition_by: ["rating"]], 
             as: "rate_variance")
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "STDDEV(film.rental_rate) OVER (PARTITION BY film.rating) AS rate_stddev"
      assert sql =~ "VARIANCE(film.rental_rate) OVER (PARTITION BY film.rating) AS rate_variance"
    end
  end
end