defmodule Selecto.StreamTest do
  use ExUnit.Case, async: true

  defmodule StreamAdapter do
    def stream(:ok, _query, _params, _opts) do
      {:ok, Stream.map([[1, "Alpha"], [2, "Beta"]], & &1), ["id", "name"]}
    end
  end

  defp domain do
    %{
      name: "Stream test",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :tenant_id],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          tenant_id: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp stream_selecto(domain_map \\ domain()) do
    domain_map
    |> Selecto.configure(nil, validate: false)
    |> Selecto.select(["id", "name"])
    |> Map.put(:adapter, StreamAdapter)
    |> Map.put(:connection, :ok)
  end

  defp materialize_stream(stream) do
    entries = Enum.to_list(stream)
    [{_first_row, columns, aliases} | _] = entries
    rows_stream = Stream.map(entries, fn {row, _columns, _aliases} -> row end)
    {rows_stream, columns, aliases}
  end

  test "execute_stream returns row stream via public API" do
    assert {:ok, stream} = Selecto.execute_stream(stream_selecto(), analyze_complexity: false)

    rows = Enum.to_list(stream)

    assert [{[1, "Alpha"], ["id", "name"], aliases_1}, {[2, "Beta"], ["id", "name"], aliases_2}] =
             rows

    assert aliases_1 == aliases_2
    assert length(aliases_1) == 2
  end

  test "execute_stream fails fast when tenant scope is required and missing" do
    tenant_domain = Map.put(domain(), :tenant_required, true)

    assert {:error, %Selecto.Error{type: :validation_error}} =
             Selecto.execute_stream(stream_selecto(tenant_domain), analyze_complexity: false)
  end

  test "execute_stream succeeds when tenant scope is required and present" do
    tenant_domain = Map.put(domain(), :tenant_required, true)

    query =
      tenant_domain
      |> stream_selecto()
      |> Selecto.with_tenant(%{tenant_id: "acme", required: true})
      |> Selecto.apply_tenant_scope()

    assert {:ok, stream} = Selecto.execute_stream(query, analyze_complexity: false)
    assert Enum.count(stream) == 2
  end

  test "execute_stream output works with stream transformers for maps and json" do
    assert {:ok, stream} = Selecto.execute_stream(stream_selecto(), analyze_complexity: false)
    {rows_stream, columns, aliases} = materialize_stream(stream)

    assert {:ok, maps_stream} =
             Selecto.Output.Formats.transform({rows_stream, columns, aliases}, {:stream, :maps},
               batch_size: 1
             )

    assert Enum.to_list(maps_stream) == [
             %{"id" => 1, "name" => "Alpha"},
             %{"id" => 2, "name" => "Beta"}
           ]

    rows_stream_json = Stream.map([[1, "Alpha"], [2, "Beta"]], & &1)

    assert {:ok, json_stream} =
             Selecto.Output.Formats.transform(
               {rows_stream_json, columns, aliases},
               {:stream, :json},
               batch_size: 1,
               format: :lines
             )

    assert json_stream
           |> Enum.map(&Jason.decode!/1)
           |> Enum.map(&Map.take(&1, ["id", "name"])) ==
             [%{"id" => 1, "name" => "Alpha"}, %{"id" => 2, "name" => "Beta"}]
  end
end
