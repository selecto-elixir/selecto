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
  end
end