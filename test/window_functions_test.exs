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

end
