defmodule Selecto.Window do
  @moduledoc """
  Window functions provide powerful analytical capabilities for PostgreSQL queries.
  
  Window functions allow you to perform calculations across a set of table rows
  that are related to the current row. Unlike aggregate functions, window functions
  do not group rows into a single output row — they retain individual row identity.
  
  ## Examples
  
      # Ranking functions
      selecto
      |> Selecto.window_function(:row_number, over: [partition_by: ["category"], order_by: ["sales_date"]])
      |> Selecto.window_function(:rank, over: [partition_by: ["region"], order_by: [{"total_sales", :desc}]])
      
      # Offset functions  
      selecto
      |> Selecto.window_function(:lag, ["sales_amount", 1], over: [partition_by: ["customer_id"], order_by: ["sales_date"]])
      |> Selecto.window_function(:lead, ["sales_amount"], over: [order_by: ["sales_date"]], as: "next_month_sales")
      
      # Aggregate window functions
      selecto  
      |> Selecto.window_function(:sum, ["sales_amount"], over: [partition_by: ["region"], order_by: ["sales_date"]])
      |> Selecto.window_function(:avg, ["sales_amount"], over: [order_by: ["sales_date"], frame: {:rows, :unbounded_preceding, :current_row}])
  """

  alias Selecto.Window.{Spec, Frame}

  defmodule Spec do
    @moduledoc """
    Specification for a window function operation.
    """
    defstruct [
      :id,                  # Unique identifier for the window function
      :function,            # Window function type (:row_number, :rank, :sum, etc.)
      :arguments,           # Arguments to the function (e.g., ["sales_amount"] for SUM)
      :partition_by,        # Fields to partition by
      :order_by,            # Fields and directions to order by
      :frame,               # Window frame specification
      :alias,               # Output column alias
      :opts                 # Additional options
    ]

    @type window_function :: :row_number | :rank | :dense_rank | :percent_rank | :ntile |
                             :lag | :lead | :first_value | :last_value |
                             :sum | :avg | :count | :min | :max | :stddev | :variance

    @type t :: %__MODULE__{
      id: String.t(),
      function: window_function(),
      arguments: [String.t()] | nil,
      partition_by: [String.t()] | nil,
      order_by: [String.t() | {String.t(), :asc | :desc}] | nil,
      frame: Frame.t() | nil,
      alias: String.t() | nil,
      opts: keyword()
    }
  end

  defmodule Frame do
    @moduledoc """
    Window frame specification (ROWS or RANGE).
    
    Defines which rows within the partition are included in the window frame
    for the current row's calculation.
    """
    defstruct [
      :type,        # :rows or :range
      :start,       # Frame start boundary
      :end          # Frame end boundary
    ]

    @type frame_type :: :rows | :range
    @type boundary :: :unbounded_preceding | :current_row | :unbounded_following |
                      {:preceding, integer()} | {:following, integer()} |
                      {:interval, String.t()}

    @type t :: %__MODULE__{
      type: frame_type(),
      start: boundary(),
      end: boundary()
    }
  end

  @doc """
  Add a window function to the Selecto query.
  
  ## Parameters
  
  - `selecto` - The Selecto struct
  - `function` - Window function type (atom)
  - `arguments` - Arguments for the function (optional for ranking functions)
  - `options` - Window function options
  
  ## Options
  
  - `:over` - Window specification (required)
    - `:partition_by` - Fields to partition by
    - `:order_by` - Fields and directions to order by  
    - `:frame` - Window frame specification
  - `:as` - Output column alias
  
  ## Examples
  
      # Row number within each category, ordered by date
      selecto |> Selecto.window_function(:row_number, 
        over: [partition_by: ["category"], order_by: ["created_at"]])
        
      # Running total of sales by customer
      selecto |> Selecto.window_function(:sum, ["amount"], 
        over: [partition_by: ["customer_id"], order_by: ["date"]], 
        as: "running_total")
        
      # Previous month's sales for comparison
      selecto |> Selecto.window_function(:lag, ["amount", 1], 
        over: [partition_by: ["customer_id"], order_by: ["month"]], 
        as: "prev_month")
  """
  def add_window_function(selecto, function, arguments \\ [], options) do
    window_spec = build_window_spec(function, arguments, options)
    
    current_windows = Map.get(selecto.set, :window_functions, [])
    updated_windows = current_windows ++ [window_spec]
    
    put_in(selecto.set[:window_functions], updated_windows)
  end

  # Build window function specification from parameters
  defp build_window_spec(function, arguments, options) do
    over_options = Keyword.get(options, :over, [])
    
    %Spec{
      id: generate_window_id(function, arguments),
      function: function,
      arguments: normalize_arguments(arguments),
      partition_by: Keyword.get(over_options, :partition_by),
      order_by: normalize_order_by(Keyword.get(over_options, :order_by)),
      frame: parse_frame(Keyword.get(over_options, :frame)),
      alias: Keyword.get(options, :as),
      opts: Keyword.drop(options, [:over, :as])
    }
  end

  # Generate unique ID for window function
  defp generate_window_id(function, arguments) do
    args_part = 
      case arguments do
        [] -> ""
        args when is_list(args) -> "_#{Enum.join(args, "_")}"
        _ -> "_args"
      end
    
    "#{function}#{args_part}_#{:erlang.unique_integer([:positive])}"
  end

  # Normalize arguments to list format
  defp normalize_arguments([]), do: nil
  defp normalize_arguments(args) when is_list(args), do: args
  defp normalize_arguments(arg), do: [arg]

  # Normalize order by specifications
  defp normalize_order_by(nil), do: nil
  defp normalize_order_by(orders) when is_list(orders) do
    Enum.map(orders, fn
      {field, direction} when direction in [:asc, :desc] -> {field, direction}
      field when is_binary(field) -> {field, :asc}
      _ -> raise ArgumentError, "Invalid order_by specification"
    end)
  end
  defp normalize_order_by(order), do: normalize_order_by([order])

  # Parse window frame specification
  defp parse_frame(nil), do: nil
  defp parse_frame({type, start_bound, end_bound}) when type in [:rows, :range] do
    %Frame{
      type: type,
      start: parse_boundary(start_bound),
      end: parse_boundary(end_bound)
    }
  end
  defp parse_frame(frame_spec) do
    raise ArgumentError, "Invalid frame specification: #{inspect(frame_spec)}"
  end

  # Parse frame boundaries
  defp parse_boundary(:unbounded_preceding), do: :unbounded_preceding
  defp parse_boundary(:current_row), do: :current_row
  defp parse_boundary(:unbounded_following), do: :unbounded_following
  defp parse_boundary({:preceding, n}) when is_integer(n) and n > 0, do: {:preceding, n}
  defp parse_boundary({:following, n}) when is_integer(n) and n > 0, do: {:following, n}
  defp parse_boundary({:interval, interval}) when is_binary(interval), do: {:interval, interval}
  defp parse_boundary(boundary) do
    raise ArgumentError, "Invalid frame boundary: #{inspect(boundary)}"
  end

  @doc """
  Parse a window frame specification.
  
  Public function for testing frame parsing.
  """
  def parse_frame_public(frame_spec) do
    parse_frame(frame_spec)
  end
end