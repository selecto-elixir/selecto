defmodule Selecto.SQL.Functions do
  @moduledoc """
  Advanced SQL function support for Selecto.
  
  This module extends the existing function support in Selecto.Builder.Sql.Select
  with additional advanced SQL functions including window functions, array operations,
  string manipulation, and mathematical functions.
  
  ## Function Categories
  
  ### String Functions
  - `substr/3` - Extract substring
  - `trim/1`, `ltrim/1`, `rtrim/1` - String trimming
  - `upper/1`, `lower/1` - Case conversion
  - `length/1` - String length
  - `position/2` - Find substring position
  - `replace/3` - String replacement
  - `split_part/3` - Split and extract part
  
  ### Mathematical Functions  
  - `abs/1` - Absolute value
  - `ceil/1`, `floor/1` - Rounding functions
  - `round/1`, `round/2` - Rounding with precision
  - `power/2` - Exponentiation
  - `sqrt/1` - Square root
  - `mod/2` - Modulo operation
  - `random/0` - Random number generation
  
  ### Date/Time Functions
  - `now/0` - Current timestamp
  - `date_trunc/2` - Truncate to date part
  - `interval/1` - Time intervals
  - `age/1`, `age/2` - Date arithmetic
  - `date_part/2` - Enhanced extract functionality
  
  ### Array Functions
  - `array_agg/1` - Array aggregation
  - `array_length/1` - Array length
  - `array_to_string/2` - Array to string conversion
  - `string_to_array/2` - String to array conversion
  - `unnest/1` - Array expansion
  - `array_cat/2` - Array concatenation
  
  ### Window Functions
  - `row_number/0` - Row numbering
  - `rank/0` - Ranking with gaps
  - `dense_rank/0` - Dense ranking
  - `lag/1`, `lag/2` - Previous row values
  - `lead/1`, `lead/2` - Next row values
  - `first_value/1`, `last_value/1` - Window boundaries
  - `ntile/1` - Percentile groups
  
  ### Conditional Functions
  - Enhanced `case` expressions
  - `decode/3+` - Oracle-style conditional
  - `iif/3` - Simple if-then-else
  
  ## Usage Examples
  
      # String functions
      {:substr, "description", 1, 50}
      {:trim, "name"}
      {:upper, "category"}
      
      # Math functions  
      {:round, "price", 2}
      {:power, "base", 2}
      
      # Window functions
      {:window, {:row_number}, over: [partition_by: ["category"], order_by: ["price"]]}
      {:window, {:lag, "price"}, over: [partition_by: ["product_id"], order_by: ["date"]]}
      
      # Array functions
      {:array_agg, "tag_name", over: [partition_by: ["product_id"]]}
      {:unnest, "tags"}
  """

  import Selecto.Builder.Sql.Helpers
  
  @doc """
  Process advanced SQL functions that extend beyond the basic set.
  
  This integrates with the existing prep_selector in Selecto.Builder.Sql.Select
  to provide comprehensive function support.
  """
  def prep_advanced_selector(selecto, selector) do
    case selector do
      # String Functions
      {:substr, field, start, length} ->
        prep_string_function(selecto, "substr", [field, {:literal, start}, {:literal, length}])
      
      {:substr, field, start} ->
        prep_string_function(selecto, "substr", [field, {:literal, start}])
        
      {:trim, field} ->
        prep_string_function(selecto, "trim", [field])
        
      {:ltrim, field} ->
        prep_string_function(selecto, "ltrim", [field])
        
      {:rtrim, field} ->
        prep_string_function(selecto, "rtrim", [field])
        
      {:upper, field} ->
        prep_string_function(selecto, "upper", [field])
        
      {:lower, field} ->
        prep_string_function(selecto, "lower", [field])
        
      {:length, field} ->
        prep_string_function(selecto, "length", [field])
        
      {:position, substring, string} ->
        prep_string_function(selecto, "position", [substring, {:literal_string, " in "}, string])
        
      {:replace, field, old, new} ->
        prep_string_function(selecto, "replace", [field, old, new])
        
      {:split_part, field, delimiter, position} ->
        prep_string_function(selecto, "split_part", [field, delimiter, {:literal, position}])
      
      # Mathematical Functions
      {:abs, field} ->
        prep_math_function(selecto, "abs", [field])
        
      {:ceil, field} ->
        prep_math_function(selecto, "ceil", [field])
        
      {:floor, field} ->
        prep_math_function(selecto, "floor", [field])
        
      {:round, field} ->
        prep_math_function(selecto, "round", [field])
        
      {:round, field, precision} ->
        prep_math_function(selecto, "round", [field, {:literal, precision}])
        
      {:power, base, exponent} ->
        prep_math_function(selecto, "power", [base, exponent])
        
      {:sqrt, field} ->
        prep_math_function(selecto, "sqrt", [field])
        
      {:mod, dividend, divisor} ->
        prep_math_function(selecto, "mod", [dividend, divisor])
        
      {:random} ->
        prep_math_function(selecto, "random", [])
      
      # Date/Time Functions  
      {:now} ->
        prep_datetime_function(selecto, "now", [])
        
      {:date_trunc, part, field} ->
        prep_datetime_function(selecto, "date_trunc", [part, field])
        
      {:interval, spec} ->
        prep_interval(selecto, spec)
        
      {:age, field} ->
        prep_datetime_function(selecto, "age", [field])
        
      {:age, field1, field2} ->
        prep_datetime_function(selecto, "age", [field1, field2])
        
      {:date_part, part, field} ->
        prep_datetime_function(selecto, "date_part", [part, field])
      
      # Array Functions
      {:array_agg, field} ->
        prep_array_function(selecto, "array_agg", [field])
        
      {:array_length, field} ->
        prep_array_function(selecto, "array_length", [field, {:literal, 1}])
        
      {:array_to_string, field, delimiter} ->
        prep_array_function(selecto, "array_to_string", [field, delimiter])
        
      {:string_to_array, field, delimiter} ->
        prep_array_function(selecto, "string_to_array", [field, delimiter])
        
      {:unnest, field} ->
        prep_array_function(selecto, "unnest", [field])
        
      {:array_cat, array1, array2} ->
        prep_array_function(selecto, "array_cat", [array1, array2])
      
      # Window Functions
      {:window, func, opts} ->
        prep_window_function(selecto, func, opts)
      
      # Enhanced Conditional Functions
      {:decode, field, mappings} ->
        prep_decode_function(selecto, field, mappings)
        
      {:iif, condition, true_value, false_value} ->
        prep_iif_function(selecto, condition, true_value, false_value)
      
      # Fallback to existing prep_selector for standard functions
      _ ->
        nil
    end
  end
  
  # String function helpers
  defp prep_string_function(selecto, func_name, args) do
    {sel_parts, joins, params} = prep_function_args(selecto, args)
    func_iodata = [func_name, "(", Enum.intersperse(sel_parts, ", "), ")"]
    {func_iodata, joins, params}
  end
  
  # Math function helpers
  defp prep_math_function(selecto, func_name, args) do
    {sel_parts, joins, params} = prep_function_args(selecto, args)
    func_iodata = [func_name, "(", Enum.intersperse(sel_parts, ", "), ")"]
    {func_iodata, joins, params}
  end
  
  # Date/time function helpers
  defp prep_datetime_function(selecto, func_name, args) do
    {sel_parts, joins, params} = prep_function_args(selecto, args)
    func_iodata = [func_name, "(", Enum.intersperse(sel_parts, ", "), ")"]
    {func_iodata, joins, params}
  end
  
  # Array function helpers
  defp prep_array_function(selecto, func_name, args) do
    {sel_parts, joins, params} = prep_function_args(selecto, args)
    func_iodata = [func_name, "(", Enum.intersperse(sel_parts, ", "), ")"]
    {func_iodata, joins, params}
  end
  
  # Window function helpers
  defp prep_window_function(selecto, func, opts) do
    # Process the base function
    {func_iodata, func_joins, func_params} = case func do
      {:row_number} ->
        {["row_number()"], [], []}
      {:rank} ->
        {["rank()"], [], []}
      {:dense_rank} ->
        {["dense_rank()"], [], []}
      {:lag, field} ->
        {field_iodata, joins, params} = prep_function_args(selecto, [field])
        {["lag(", field_iodata, ")"], joins, params}
      {:lag, field, offset} ->
        {args_iodata, joins, params} = prep_function_args(selecto, [field, {:literal, offset}])
        {["lag(", Enum.intersperse(args_iodata, ", "), ")"], joins, params}
      {:lead, field} ->
        {field_iodata, joins, params} = prep_function_args(selecto, [field])
        {["lead(", field_iodata, ")"], joins, params}
      {:lead, field, offset} ->
        {args_iodata, joins, params} = prep_function_args(selecto, [field, {:literal, offset}])
        {["lead(", Enum.intersperse(args_iodata, ", "), ")"], joins, params}
      {:first_value, field} ->
        {field_iodata, joins, params} = prep_function_args(selecto, [field])
        {["first_value(", field_iodata, ")"], joins, params}
      {:last_value, field} ->
        {field_iodata, joins, params} = prep_function_args(selecto, [field])
        {["last_value(", field_iodata, ")"], joins, params}
      {:ntile, buckets} ->
        {["ntile(", Integer.to_string(buckets), ")"], [], []}
      {agg_func, field} when agg_func in [:sum, :count, :avg, :min, :max] ->
        {field_iodata, joins, params} = prep_function_args(selecto, [field])
        func_name = Atom.to_string(agg_func)
        {[func_name, "(", field_iodata, ")"], joins, params}
    end
    
    # Build OVER clause
    {over_iodata, over_joins, over_params} = build_over_clause(selecto, opts)
    
    # Combine function with OVER clause
    window_iodata = [func_iodata, " over (", over_iodata, ")"]
    all_joins = List.flatten([func_joins | over_joins])
    all_params = func_params ++ over_params
    
    {window_iodata, all_joins, all_params}
  end
  
  # Build OVER clause for window functions
  defp build_over_clause(selecto, opts) do
    partition_clause = case Keyword.get(opts, :partition_by) do
      nil -> []
      fields when is_list(fields) ->
        {part_args, part_joins, part_params} = prep_function_args(selecto, fields)
        {["partition by ", Enum.intersperse(part_args, ", ")], part_joins, part_params}
      field ->
        {part_args, part_joins, part_params} = prep_function_args(selecto, [field])
        {["partition by ", part_args], part_joins, part_params}
    end
    
    order_clause = case Keyword.get(opts, :order_by) do
      nil -> []
      fields when is_list(fields) ->
        {order_args, order_joins, order_params} = prep_function_args(selecto, fields)
        {["order by ", Enum.intersperse(order_args, ", ")], order_joins, order_params}
      field ->
        {order_args, order_joins, order_params} = prep_function_args(selecto, [field])
        {["order by ", order_args], order_joins, order_params}
    end
    
    # Combine clauses
    case {partition_clause, order_clause} do
      {[], []} ->
        {[], [], []}
      {{part_iodata, part_joins, part_params}, []} ->
        {part_iodata, part_joins, part_params}
      {[], {order_iodata, order_joins, order_params}} ->
        {order_iodata, order_joins, order_params}
      {{part_iodata, part_joins, part_params}, {order_iodata, order_joins, order_params}} ->
        combined_iodata = [part_iodata, " ", order_iodata]
        combined_joins = part_joins ++ order_joins
        combined_params = part_params ++ order_params
        {combined_iodata, combined_joins, combined_params}
    end
  end
  
  # Interval function helper
  defp prep_interval(selecto, spec) when is_binary(spec) do
    # Handle PostgreSQL interval syntax: "1 day", "2 hours", etc.
    interval_iodata = ["interval '", spec, "'"]
    {interval_iodata, [], []}
  end
  
  defp prep_interval(selecto, {amount, unit}) do
    # Handle tuple format: {1, "day"}, {2, "hour"}, etc.
    interval_iodata = ["interval '", Integer.to_string(amount), " ", unit, "'"]
    {interval_iodata, [], []}
  end
  
  # Decode function (Oracle-style conditional)
  defp prep_decode_function(selecto, field, mappings) do
    {field_iodata, field_joins, field_params} = prep_function_args(selecto, [field])
    
    {mapping_parts, mapping_joins, mapping_params} = 
      Enum.reduce(mappings, {[], [], []}, fn {match_value, return_value}, {parts, joins, params} ->
        {match_iodata, match_joins, match_params} = prep_function_args(selecto, [match_value])
        {return_iodata, return_joins, return_params} = prep_function_args(selecto, [return_value])
        
        new_parts = parts ++ [match_iodata, ", ", return_iodata]
        new_joins = joins ++ match_joins ++ return_joins
        new_params = params ++ match_params ++ return_params
        
        {new_parts, new_joins, new_params}
      end)
    
    decode_iodata = ["decode(", field_iodata, ", ", mapping_parts, ")"]
    all_joins = field_joins ++ mapping_joins
    all_params = field_params ++ mapping_params
    
    {decode_iodata, all_joins, all_params}
  end
  
  # Simple if-then-else function
  defp prep_iif_function(selecto, condition, true_value, false_value) do
    # Convert to CASE expression
    case_selector = {:case, [{condition, true_value}], false_value}
    
    # Delegate to existing case handling (would need to call back to main prep_selector)
    # For now, build manually
    {cond_iodata, cond_joins, cond_params} = build_condition_iodata(selecto, condition)
    {true_iodata, true_joins, true_params} = prep_function_args(selecto, [true_value])
    {false_iodata, false_joins, false_params} = prep_function_args(selecto, [false_value])
    
    case_iodata = [
      "case when ", cond_iodata, " then ", true_iodata, 
      " else ", false_iodata, " end"
    ]
    
    all_joins = cond_joins ++ true_joins ++ false_joins
    all_params = cond_params ++ true_params ++ false_params
    
    {case_iodata, all_joins, all_params}
  end
  
  # Helper to build condition iodata (simplified for now)
  defp build_condition_iodata(selecto, condition) do
    # This would need to integrate with the WHERE clause builder
    # For now, handle simple field comparisons
    case condition do
      {field, :eq, value} ->
        {field_iodata, field_joins, field_params} = prep_function_args(selecto, [field])
        {value_iodata, value_joins, value_params} = prep_function_args(selecto, [value])
        condition_iodata = [field_iodata, " = ", value_iodata]
        {condition_iodata, field_joins ++ value_joins, field_params ++ value_params}
      
      {field, :gt, value} ->
        {field_iodata, field_joins, field_params} = prep_function_args(selecto, [field])
        {value_iodata, value_joins, value_params} = prep_function_args(selecto, [value])
        condition_iodata = [field_iodata, " > ", value_iodata]
        {condition_iodata, field_joins ++ value_joins, field_params ++ value_params}
      
      # Add more condition types as needed
      _ ->
        {["true"], [], []}  # Fallback
    end
  end
  
  # Generic function argument processor
  defp prep_function_args(selecto, args) do
    Enum.reduce(args, {[], [], []}, fn arg, {sel_parts, joins, params} ->
      {arg_iodata, arg_joins, arg_params} = prep_single_arg(selecto, arg)
      
      {
        sel_parts ++ [arg_iodata],
        joins ++ List.wrap(arg_joins),
        params ++ arg_params
      }
    end)
  end
  
  # Process individual function arguments
  defp prep_single_arg(selecto, arg) do
    case arg do
      {:literal, value} ->
        {{:param, value}, :selecto_root, [value]}
        
      {:literal_string, value} ->
        {["'", String.replace(value, "'", "''"), "'"], :selecto_root, []}
        
      field when is_binary(field) ->
        # Handle field references by calling back to main prep_selector
        Selecto.Builder.Sql.Select.prep_selector(selecto, field)
        
      value when is_integer(value) or is_float(value) or is_boolean(value) ->
        {{:param, value}, :selecto_root, [value]}
        
      # For complex expressions, call back to main prep_selector
      complex_expr ->
        case prep_advanced_selector(selecto, complex_expr) do
          nil -> 
            # Try the main prep_selector for standard expressions
            try do
              Selecto.Builder.Sql.Select.prep_selector(selecto, complex_expr)
            rescue
              _ -> {[inspect(complex_expr)], :selecto_root, []}
            end
          result -> result
        end
    end
  end
end