defmodule Selecto.Builder.Sql.HelpersTest do
  use ExUnit.Case, async: true

  alias Selecto.Builder.Sql.Helpers

  test "maybe_quote_identifier escapes embedded ANSI identifier quotes" do
    assert Helpers.maybe_quote_identifier(~s(report"name)) == ~s("report""name")
  end

  test "quote_identifier delegates escaping to the configured adapter" do
    selecto = %{adapter: Selecto.DB.PostgreSQL}

    assert Helpers.quote_identifier(selecto, "Report Name") == ~s("Report Name")
  end
end
