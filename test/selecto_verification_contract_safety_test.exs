defmodule Selecto.Verification.ContractSafetyTest do
  use ExUnit.Case, async: true

  alias Selecto.Verification.ContractSafety

  test "proves the provider-consumer compatibility matrix" do
    report = ContractSafety.verify()

    assert report.state_count == 32
    assert report.invariant_count == 2
    assert report.check_count == 64
    assert report.proved?, inspect(report.counterexamples, pretty: true)
  end
end
