defmodule Selecto.Performance.MetricsCollectorTest do
  use ExUnit.Case, async: false

  alias Selecto.Performance.MetricsCollector

  setup do
    {:ok, _pid} = MetricsCollector.start_link()
    :ok
  end

  test "records query metrics and returns stats" do
    MetricsCollector.record_query("q1", %{query_id: "q1", execution_time: 10, row_count: 5})
    MetricsCollector.record_query("q2", %{query_id: "q2", execution_time: 20, row_count: 10})

    stats = MetricsCollector.get_stats()
    assert is_map(stats)
  end

  test "exports metrics in json and csv" do
    MetricsCollector.record_query("q3", %{query_id: "q3", execution_time: 15, row_count: 2})

    assert {:ok, json} = MetricsCollector.export_metrics(:json)
    assert is_binary(json)

    assert {:ok, csv} = MetricsCollector.export_metrics(:csv)
    assert is_binary(csv)
  end
end
