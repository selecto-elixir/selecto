defmodule Selecto.ErrorTest do
  use ExUnit.Case

  alias Selecto.Error

  test "constructor helpers build expected error types" do
    assert %Error{type: :connection_error} = Error.connection_error("nope")

    assert %Error{type: :query_error, query: "select 1"} =
             Error.query_error("bad", "select 1", [1], %{a: 1})

    assert %Error{type: :validation_error} = Error.validation_error("invalid")
    assert %Error{type: :configuration_error} = Error.configuration_error("bad config")
    assert %Error{type: :no_results} = Error.no_results_error()
    assert %Error{type: :multiple_results} = Error.multiple_results_error()
    assert %Error{type: :timeout_error} = Error.timeout_error("slow")

    assert %Error{type: :field_resolution_error} =
             Error.field_resolution_error("missing", "x", %{ctx: 1})

    assert %Error{type: :transformation_error} = Error.transformation_error("format")
  end

  test "from_reason converts common reasons" do
    assert %Error{type: :connection_error} = Error.from_reason({:exit, :killed})
    assert %Error{type: :no_results} = Error.from_reason(:no_results)
    assert %Error{type: :multiple_results} = Error.from_reason(:multiple_results)
    assert %Error{type: :query_error, message: "boom"} = Error.from_reason("boom")
    assert %Error{type: :query_error, details: %{reason: :other}} = Error.from_reason(:other)
  end

  test "from_reason handles exceptions" do
    ex = RuntimeError.exception("broken")
    result = Error.from_reason(ex)

    assert %Error{type: :query_error, message: "broken"} = result
    assert %{exception: ^ex} = result.details
  end

  test "to_exception formats connection errors differently" do
    conn_ex = Error.connection_error("down") |> Error.to_exception()
    generic_ex = Error.query_error("bad sql") |> Error.to_exception()

    assert %RuntimeError{message: "Database connection failed: down"} = conn_ex
    assert %RuntimeError{message: "bad sql"} = generic_ex
  end

  test "display messages are user-friendly" do
    assert "Database connection failed: down" ==
             Error.to_display_message(Error.connection_error("down"))

    assert "Query execution failed: nope" == Error.to_display_message(Error.query_error("nope"))

    assert "Validation error: invalid" ==
             Error.to_display_message(Error.validation_error("invalid"))

    assert "Configuration error: config" ==
             Error.to_display_message(Error.configuration_error("config"))

    assert "No results found" == Error.to_display_message(Error.no_results_error())

    assert "Expected one result, but got multiple" ==
             Error.to_display_message(Error.multiple_results_error())

    assert "Query timeout: slow" == Error.to_display_message(Error.timeout_error("slow"))

    assert "Field resolution error: bad field" ==
             Error.to_display_message(Error.field_resolution_error("bad field", "x"))

    assert "Output format transformation error: bad output" ==
             Error.to_display_message(Error.transformation_error("bad output"))
  end
end
