defmodule Selecto.HelpersTest do
  use ExUnit.Case
  alias Selecto.Helpers

  describe "check_safe_phrase/1" do
    test "accepts valid alphanumeric strings" do
      assert Helpers.check_safe_phrase("hello") == "hello"
      assert Helpers.check_safe_phrase("Hello123") == "Hello123"
      assert Helpers.check_safe_phrase("valid_name") == "valid_name"
      assert Helpers.check_safe_phrase("test value") == "test value"
    end

    test "returns nil for nil input" do
      assert Helpers.check_safe_phrase(nil) == nil
    end

    test "converts integers to strings and validates" do
      assert Helpers.check_safe_phrase(123) == "123"
      assert Helpers.check_safe_phrase(42) == "42"
    end

    test "converts floats to strings and validates" do
      # Floats will contain a period which is a special character
      assert_raise RuntimeError, fn ->
        Helpers.check_safe_phrase(3.14)
      end
    end

    test "converts atoms to strings and validates" do
      assert Helpers.check_safe_phrase(:hello) == "hello"
      assert Helpers.check_safe_phrase(:valid_name) == "valid_name"
    end

    test "raises error for empty strings" do
      assert_raise RuntimeError, "Invalid String ", fn ->
        Helpers.check_safe_phrase("")
      end
    end

    test "raises error for strings with special characters" do
      assert_raise RuntimeError, "Invalid String hello@world", fn ->
        Helpers.check_safe_phrase("hello@world")
      end

      assert_raise RuntimeError, "Invalid String test-value", fn ->
        Helpers.check_safe_phrase("test-value")
      end

      assert_raise RuntimeError, "Invalid String value;", fn ->
        Helpers.check_safe_phrase("value;")
      end

      assert_raise RuntimeError, "Invalid String 'quoted'", fn ->
        Helpers.check_safe_phrase("'quoted'")
      end
    end

    test "raises error for SQL injection attempts" do
      assert_raise RuntimeError, fn ->
        Helpers.check_safe_phrase("'; DROP TABLE users; --")
      end

      assert_raise RuntimeError, fn ->
        Helpers.check_safe_phrase("1 OR 1=1")
      end
    end
  end
end
