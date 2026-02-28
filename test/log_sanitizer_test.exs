defmodule Selecto.LogSanitizerTest do
  use ExUnit.Case
  alias Selecto.LogSanitizer

  describe "sanitize_query/3" do
    test "returns [no query] for nil input" do
      assert LogSanitizer.sanitize_query(nil, []) == "[no query]"
    end

    test "returns query without param note when no params" do
      assert LogSanitizer.sanitize_query("SELECT * FROM users", []) == "SELECT * FROM users"
    end

    test "appends param count for queries with params" do
      result = LogSanitizer.sanitize_query("SELECT * FROM users WHERE id = $1", [123])
      assert result == "SELECT * FROM users WHERE id = $1 [1 param(s) redacted]"
    end

    test "handles multiple params" do
      result =
        LogSanitizer.sanitize_query(
          "SELECT * FROM users WHERE id = $1 AND name = $2",
          [123, "test"]
        )

      assert result == "SELECT * FROM users WHERE id = $1 AND name = $2 [2 param(s) redacted]"
    end

    test "truncates long queries" do
      long_query = String.duplicate("a", 600)
      result = LogSanitizer.sanitize_query(long_query, [])
      assert String.length(result) < 600
      assert String.ends_with?(result, "...")
    end

    test "respects max_length option" do
      query = "SELECT * FROM users WHERE id = $1"
      result = LogSanitizer.sanitize_query(query, [1], max_length: 10)
      assert String.starts_with?(result, "SELECT * F...")
    end

    test "can hide param count with show_param_count: false" do
      result =
        LogSanitizer.sanitize_query(
          "SELECT * FROM users WHERE id = $1",
          [123],
          show_param_count: false
        )

      assert result == "SELECT * FROM users WHERE id = $1"
    end

    test "handles non-string queries by inspecting them" do
      result = LogSanitizer.sanitize_query(%{query: "test"}, [])
      assert is_binary(result)
    end
  end

  describe "sanitize_params/1" do
    test "returns [0 params] for empty list" do
      assert LogSanitizer.sanitize_params([]) == "[0 params]"
    end

    test "shows param count and types" do
      result = LogSanitizer.sanitize_params([1, "hello", nil])
      assert result == "[3 param(s): integer, binary, nil]"
    end

    test "identifies boolean type" do
      result = LogSanitizer.sanitize_params([true, false])
      assert result == "[2 param(s): boolean, boolean]"
    end

    test "identifies float type" do
      result = LogSanitizer.sanitize_params([3.14, 2.71])
      assert result == "[2 param(s): float, float]"
    end

    test "identifies atom type" do
      result = LogSanitizer.sanitize_params([:foo, :bar])
      assert result == "[2 param(s): atom, atom]"
    end

    test "identifies list type" do
      result = LogSanitizer.sanitize_params([[1, 2], [3, 4]])
      assert result == "[2 param(s): list, list]"
    end

    test "identifies map type" do
      result = LogSanitizer.sanitize_params([%{a: 1}, %{b: 2}])
      assert result == "[2 param(s): map, map]"
    end

    test "identifies date types" do
      result = LogSanitizer.sanitize_params([~D[2024-01-15]])
      assert result == "[1 param(s): date]"
    end

    test "identifies datetime types" do
      result = LogSanitizer.sanitize_params([~U[2024-01-15 10:30:00Z]])
      assert result == "[1 param(s): datetime]"
    end

    test "identifies naive_datetime types" do
      result = LogSanitizer.sanitize_params([~N[2024-01-15 10:30:00]])
      assert result == "[1 param(s): naive_datetime]"
    end

    test "identifies time types" do
      result = LogSanitizer.sanitize_params([~T[10:30:00]])
      assert result == "[1 param(s): time]"
    end

    test "identifies decimal types" do
      result = LogSanitizer.sanitize_params([Decimal.new("123.45")])
      assert result == "[1 param(s): decimal]"
    end

    test "truncates types list for many params" do
      params = Enum.map(1..15, fn _ -> 1 end)
      result = LogSanitizer.sanitize_params(params)
      assert result =~ "[15 param(s):"
      assert result =~ "..."
    end

    test "returns [unknown params] for non-list input" do
      assert LogSanitizer.sanitize_params("not a list") == "[unknown params]"
    end
  end

  describe "sanitize_error/1" do
    test "sanitizes exception errors" do
      error = %RuntimeError{message: "something went wrong"}
      result = LogSanitizer.sanitize_error(error)
      assert result =~ "RuntimeError"
      assert result =~ "something went wrong"
    end

    test "truncates long error messages" do
      long_message = String.duplicate("a", 300)
      error = %RuntimeError{message: long_message}
      result = LogSanitizer.sanitize_error(error)
      assert String.length(result) < 300
      assert result =~ "..."
    end

    test "sanitizes binary errors" do
      result = LogSanitizer.sanitize_error("simple error message")
      assert result == "simple error message"
    end

    test "truncates long binary errors" do
      long_error = String.duplicate("x", 300)
      result = LogSanitizer.sanitize_error(long_error)
      assert String.length(result) < 300
      assert result =~ "..."
    end

    test "handles other error types with inspect" do
      result = LogSanitizer.sanitize_error({:error, :some_reason})
      assert is_binary(result)
    end
  end

  describe "safe_context/1" do
    test "sanitizes query in context" do
      context = [query: "SELECT * FROM users", params: [1, 2, 3]]
      result = LogSanitizer.safe_context(context)
      assert result[:query] =~ "[3 param(s) redacted]"
      assert result[:params] == "[redacted]"
    end

    test "sanitizes sql key like query" do
      context = [sql: "SELECT * FROM users", params: [1]]
      result = LogSanitizer.safe_context(context)
      assert result[:sql] =~ "[1 param(s) redacted]"
    end

    test "sanitizes error in context" do
      context = [error: %RuntimeError{message: "test error"}]
      result = LogSanitizer.safe_context(context)
      assert result[:error] =~ "RuntimeError"
    end

    test "sanitizes reason in context" do
      context = [reason: %RuntimeError{message: "test reason"}]
      result = LogSanitizer.safe_context(context)
      assert result[:reason] =~ "RuntimeError"
    end

    test "redacts sensitive keys" do
      context = [
        password: "secret123",
        secret: "api_key",
        token: "jwt_token",
        key: "encryption_key",
        credential: "creds"
      ]

      result = LogSanitizer.safe_context(context)

      assert result[:password] == "[redacted]"
      assert result[:secret] == "[redacted]"
      assert result[:token] == "[redacted]"
      assert result[:key] == "[redacted]"
      assert result[:credential] == "[redacted]"
    end

    test "passes through other keys unchanged" do
      context = [user_id: 123, action: "select", table: "users"]
      result = LogSanitizer.safe_context(context)

      assert result[:user_id] == 123
      assert result[:action] == "select"
      assert result[:table] == "users"
    end
  end
end
