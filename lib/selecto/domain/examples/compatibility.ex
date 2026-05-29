defmodule Selecto.Domain.Examples.Compatibility do
  @moduledoc """
  Compatibility artifacts and expectations for canonical domain examples.

  The examples themselves live in `Selecto.Domain.Examples`. This module adds a
  stable, package-neutral matrix that downstream packages can assert against
  without copying large JSON blobs into each repository.
  """

  alias Selecto.Domain
  alias Selecto.Domain.Examples

  @domain_ids [:work_items, :camp_registrations]

  @doc """
  Returns the canonical example domains keyed by stable domain id.
  """
  @spec domains() :: %{atom() => map()}
  def domains do
    %{
      work_items: Examples.work_items(),
      camp_registrations: Examples.camp_registrations()
    }
  end

  @doc """
  Returns the stable semantic expectations for each canonical domain.
  """
  @spec expectations() :: %{atom() => map()}
  def expectations do
    %{
      work_items: %{
        capability_ids: [
          "selecto.exported_views.manage",
          "selecto.exports.download",
          "selecto.scheduled_exports.manage",
          "work_items.archive",
          "work_items.assign",
          "work_items.private_metric",
          "work_items.published_views",
          "work_items.read"
        ],
        field_ids: [
          "archived_at",
          "id",
          "owner_id",
          "owners.active",
          "owners.id",
          "owners.name",
          "owners.tenant_id",
          "priority",
          "private_metric",
          "state",
          "tenant_id",
          "title"
        ],
        filter_ids: ["owner", "private_metric", "state"],
        choice_source_ids: ["owner_choices", "state_choices"],
        published_view_ids: ["manager_rollup"],
        write_operation_ids: ["soft_delete", "update"],
        write_field_ids: ["archived_at", "owner_id", "priority", "state"],
        action_ids: ["archive"],
        action_inputs: %{"archive" => ["reason"]},
        action_capabilities: %{"archive" => "work_items.archive"},
        tenant_scope: %{required: true, field: "tenant_id"}
      },
      camp_registrations: %{
        capability_ids: [
          "camp.cabins.assign",
          "camp.check_in",
          "camp.published_views",
          "camp.registrations.read",
          "selecto.exported_views.manage",
          "selecto.exports.download",
          "selecto.scheduled_exports.manage"
        ],
        field_ids: [
          "balance_cents",
          "cabin_id",
          "cabins.id",
          "cabins.name",
          "cabins.open",
          "cabins.session_id",
          "camper_first_name",
          "checked_in_at",
          "documents_complete",
          "id",
          "medical_form_received",
          "status",
          "tenant_id"
        ],
        filter_ids: ["cabin", "status"],
        choice_source_ids: ["cabin_choices"],
        published_view_ids: ["check_in_roster"],
        write_operation_ids: ["update"],
        write_field_ids: ["cabin_id", "checked_in_at", "documents_complete", "status"],
        action_ids: ["check_in_camper"],
        action_inputs: %{"check_in_camper" => ["checked_in_at", "documents_complete"]},
        action_capabilities: %{"check_in_camper" => "camp.check_in"},
        tenant_scope: %{required: true, field: "tenant_id"}
      }
    }
  end

  @doc """
  Builds JSON-safe core query-contract artifacts for all canonical domains.
  """
  @spec query_contract_artifacts(keyword()) :: {:ok, map()} | {:error, map()}
  def query_contract_artifacts(opts \\ []) do
    domains()
    |> Enum.reduce_while({:ok, %{}}, fn {domain_id, domain}, {:ok, acc} ->
      case query_contract_artifact(domain_id, domain, opts) do
        {:ok, artifact} -> {:cont, {:ok, Map.put(acc, domain_id, artifact)}}
        {:error, diagnostics} -> {:halt, {:error, %{domain: domain_id, diagnostics: diagnostics}}}
      end
    end)
  end

  @doc """
  Builds a JSON-safe core query-contract artifact for one canonical domain.
  """
  @spec query_contract_artifact(atom(), map(), keyword()) ::
          {:ok, map()} | {:error, Selecto.Domain.Diagnostics.t()}
  def query_contract_artifact(domain_id, domain, opts \\ []) when domain_id in @domain_ids do
    with {:ok, contract, diagnostics} <- Domain.query_contract(domain) do
      artifact =
        contract
        |> Map.put(:domain_id, domain_id)
        |> Map.put(:generated_at, Keyword.get(opts, :generated_at, "stable"))
        |> json_safe()

      {:ok, artifact, diagnostics}
      |> case do
        {:ok, artifact, %{errors: []}} -> {:ok, artifact}
        {:ok, _artifact, diagnostics} -> {:error, diagnostics}
      end
    end
  end

  @doc """
  Checks core query artifacts against the compatibility matrix.
  """
  @spec check() :: :ok | {:error, [map()]}
  def check do
    with {:ok, artifacts} <- query_contract_artifacts() do
      errors =
        artifacts
        |> Enum.flat_map(fn {domain_id, artifact} ->
          expected = Map.fetch!(expectations(), domain_id)
          query_summary = query_summary(artifact)

          [
            compare(
              domain_id,
              :capability_ids,
              expected.capability_ids,
              query_summary.capability_ids
            ),
            compare(domain_id, :field_ids, expected.field_ids, query_summary.field_ids),
            compare(domain_id, :filter_ids, expected.filter_ids, query_summary.filter_ids),
            compare(
              domain_id,
              :choice_source_ids,
              expected.choice_source_ids,
              query_summary.choice_source_ids
            ),
            compare(
              domain_id,
              :published_view_ids,
              expected.published_view_ids,
              query_summary.published_view_ids
            )
          ]
          |> Enum.reject(&is_nil/1)
        end)

      if errors == [], do: :ok, else: {:error, errors}
    end
  end

  @doc """
  Summarizes a JSON-safe query artifact into stable semantic ids.
  """
  @spec query_summary(map()) :: map()
  def query_summary(artifact) when is_map(artifact) do
    %{
      capability_ids: artifact |> map_get("capability_ids", []) |> sorted_string_ids(),
      field_ids: artifact |> map_get("fields", []) |> entry_ids(),
      filter_ids: artifact |> map_get("filters", []) |> entry_ids(),
      choice_source_ids: artifact |> map_get("choice_sources", []) |> entry_ids(),
      published_view_ids: artifact |> map_get("published_views", []) |> entry_ids()
    }
  end

  defp compare(_domain_id, _key, expected, expected), do: nil

  defp compare(domain_id, key, expected, actual) do
    %{domain: domain_id, key: key, expected: expected, actual: actual}
  end

  defp entry_ids(entries) when is_list(entries) do
    entries
    |> Enum.map(&map_get(&1, "id"))
    |> sorted_string_ids()
  end

  defp entry_ids(_entries), do: []

  defp sorted_string_ids(values) when is_list(values) do
    values
    |> Enum.map(&to_string/1)
    |> Enum.sort()
  end

  defp sorted_string_ids(_values), do: []

  defp json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_safe(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp json_safe(%Date{} = value), do: Date.to_iso8601(value)
  defp json_safe(%Time{} = value), do: Time.to_iso8601(value)
  defp json_safe(value) when is_atom(value), do: Atom.to_string(value)
  defp json_safe(value) when is_tuple(value), do: value |> Tuple.to_list() |> json_safe()
  defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)

  defp json_safe(value) when is_map(value) do
    value
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new(fn {key, value} -> {to_string(key), json_safe(value)} end)
  end

  defp json_safe(value), do: value

  defp map_get(map, key, default \\ nil)
  defp map_get(map, key, default) when is_map(map), do: Map.get(map, key, default)
  defp map_get(_map, _key, default), do: default
end
