defmodule Selecto.Verification.BoundedModelTest do
  use ExUnit.Case, async: true

  alias Selecto.Verification.BoundedModel

  test "proves every invariant across the complete supplied model" do
    report =
      BoundedModel.check("booleans", [false, true], [
        {"is_boolean", fn state -> is_boolean(state) end},
        {"double_negation", fn state -> not not state == state end}
      ])

    assert report.proof_level == :bounded_exhaustive
    assert report.state_count == 2
    assert report.invariant_count == 2
    assert report.check_count == 4
    assert report.proved?
    assert report.counterexamples == []
  end

  test "returns deterministic, reproducible counterexamples" do
    report =
      BoundedModel.check("small integers", 0..2, [
        {"less_than_two", fn state -> state < 2 end}
      ])

    refute report.proved?

    assert report.counterexamples == [
             %{
               invariant: "less_than_two",
               invariant_index: 0,
               state_index: 2,
               state: 2,
               reason: :returned_false
             }
           ]
  end

  test "captures invariant exceptions instead of aborting the proof run" do
    report =
      BoundedModel.check("exceptions", [:bad], [
        {"safe", fn _ -> raise "boom" end}
      ])

    assert [
             %{
               reason: %{tuple: [:exception, RuntimeError, "boom"]}
             }
           ] = report.counterexamples
  end

  test "counterexamples remain JSON serializable for artifact output" do
    report =
      BoundedModel.check("portable", [%{tuple: {:tenant_id, 7}, callback: fn -> :ok end}], [
        {"fails", fn _ -> false end}
      ])

    assert {:ok, _json} = Jason.encode(report)
  end
end
