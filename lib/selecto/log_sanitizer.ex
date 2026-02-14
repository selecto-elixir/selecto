defmodule Selecto.LogSanitizer do
  @moduledoc """
  Sanitizes SQL queries and parameters for safe logging.

  This module ensures that sensitive data from query parameters is never
  written to logs, while still providing useful debugging information.

  ## Security

  - Parameters are replaced with placeholders like `$1`, `$2`, etc.
  - Parameter values are NEVER logged
  - SQL structure is preserved for debugging purposes
  - Query previews are truncated to prevent log bloat

  ## Usage

      iex> LogSanitizer.sanitize_query("SELECT * FROM users WHERE id = $1", [123])
      "SELECT * FROM users WHERE id = $1 [1 param(s) redacted]"

      iex> LogSanitizer.sanitize_params([1, "secret", %{key: "value"}])
      "[3 param(s) redacted]"
  """

  @doc """
  Sanitizes a query string for logging, optionally with parameter count.

  Returns the query with a note about redacted parameters.

  ## Options

  - `:max_length` - Maximum length of query to include (default: 500)
  - `:show_param_count` - Whether to show parameter count (default: true)

  ## Examples

      iex> sanitize_query("SELECT * FROM users WHERE id = $1", [123])
      "SELECT * FROM users WHERE id = $1 [1 param(s) redacted]"

      iex> sanitize_query("SELECT * FROM users", [])
      "SELECT * FROM users"
  """
  @spec sanitize_query(String.t() | nil, list(), keyword()) :: String.t()
  def sanitize_query(query, params \\ [], opts \\ [])

  def sanitize_query(nil, _params, _opts), do: "[no query]"

  def sanitize_query(query, params, opts) when is_binary(query) do
    max_length = Keyword.get(opts, :max_length, 500)
    show_param_count = Keyword.get(opts, :show_param_count, true)

    truncated_query = truncate_query(query, max_length)
    param_count = length(List.wrap(params))

    if show_param_count && param_count > 0 do
      "#{truncated_query} [#{param_count} param(s) redacted]"
    else
      truncated_query
    end
  end

  def sanitize_query(query, params, opts) do
    sanitize_query(inspect(query), params, opts)
  end

  @doc """
  Returns a safe representation of parameters for logging.

  NEVER logs actual parameter values - only the count and types.

  ## Examples

      iex> sanitize_params([1, "secret", nil])
      "[3 param(s): integer, binary, nil]"

      iex> sanitize_params([])
      "[0 params]"
  """
  @spec sanitize_params(list()) :: String.t()
  def sanitize_params(params) when is_list(params) do
    count = length(params)

    if count == 0 do
      "[0 params]"
    else
      types = params |> Enum.take(10) |> Enum.map(&param_type/1) |> Enum.join(", ")
      suffix = if count > 10, do: ", ...", else: ""
      "[#{count} param(s): #{types}#{suffix}]"
    end
  end

  def sanitize_params(_), do: "[unknown params]"

  @doc """
  Sanitizes an error for logging, removing any embedded parameter values.

  ## Examples

      iex> sanitize_error(%Postgrex.Error{message: "error"})
      "%Postgrex.Error{message: \\"error\\"}"
  """
  @spec sanitize_error(term()) :: String.t()
  def sanitize_error(error) when is_exception(error) do
    # Only include the error type and message, not full struct which might contain params
    error_type = error.__struct__ |> Module.split() |> Enum.join(".")
    message = Map.get(error, :message, "unknown")
    "#{error_type}: #{truncate_string(to_string(message), 200)}"
  end

  def sanitize_error(error) when is_binary(error) do
    truncate_string(error, 200)
  end

  def sanitize_error(error) do
    # For other error types, use limited inspect to avoid leaking data
    inspect(error, limit: 5, printable_limit: 100)
  end

  @doc """
  Creates a safe log context map with sanitized values.

  ## Examples

      iex> safe_context(query: "SELECT...", params: [1,2,3], error: %RuntimeError{})
      %{query: "SELECT... [3 param(s) redacted]", error: "RuntimeError: ..."}
  """
  @spec safe_context(keyword()) :: map()
  def safe_context(context) when is_list(context) do
    context
    |> Enum.map(fn
      {:query, q} -> {:query, sanitize_query(q, Keyword.get(context, :params, []))}
      {:sql, q} -> {:sql, sanitize_query(q, Keyword.get(context, :params, []))}
      {:params, _} -> {:params, "[redacted]"}
      {:error, e} -> {:error, sanitize_error(e)}
      {:reason, r} -> {:reason, sanitize_error(r)}
      {k, _v} when k in [:password, :secret, :token, :key, :credential] -> {k, "[redacted]"}
      other -> other
    end)
    |> Map.new()
  end

  # Private functions

  defp truncate_query(query, max_length) when byte_size(query) > max_length do
    String.slice(query, 0, max_length) <> "..."
  end

  defp truncate_query(query, _max_length), do: query

  defp truncate_string(str, max_length) when is_binary(str) and byte_size(str) > max_length do
    String.slice(str, 0, max_length) <> "..."
  end

  defp truncate_string(str, _max_length), do: to_string(str)

  defp param_type(nil), do: "nil"
  defp param_type(p) when is_integer(p), do: "integer"
  defp param_type(p) when is_float(p), do: "float"
  defp param_type(p) when is_binary(p), do: "binary"
  defp param_type(p) when is_boolean(p), do: "boolean"
  defp param_type(p) when is_atom(p), do: "atom"
  defp param_type(p) when is_list(p), do: "list"
  # Struct patterns must come before the generic is_map guard
  defp param_type(%Date{}), do: "date"
  defp param_type(%DateTime{}), do: "datetime"
  defp param_type(%NaiveDateTime{}), do: "naive_datetime"
  defp param_type(%Time{}), do: "time"
  defp param_type(%Decimal{}), do: "decimal"
  defp param_type(p) when is_map(p), do: "map"
  defp param_type(_), do: "other"
end
