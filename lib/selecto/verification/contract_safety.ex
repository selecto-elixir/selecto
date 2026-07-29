defmodule Selecto.Verification.ContractSafety do
  @moduledoc """
  Bounded verification of provider/consumer contract compatibility.

  The model exhaustively crosses valid and invalid field, filter, required
  scope, version, and fingerprint dependencies. It proves that compatible
  consumers are accepted and every incompatible dimension is rejected with
  its specific diagnostic.
  """

  alias Selecto.Domain.ContractVerification
  alias Selecto.Verification.BoundedModel

  @doc """
  Runs the built-in contract compatibility model.
  """
  @spec verify() :: BoundedModel.report()
  def verify do
    BoundedModel.check("selecto.domain_contract_compatibility.v1", states(), invariants())
  end

  defp states do
    for field_valid? <- [false, true],
        filter_valid? <- [false, true],
        scope_satisfied? <- [false, true],
        version_valid? <- [false, true],
        fingerprint_valid? <- [false, true] do
      dimensions = %{
        field_valid?: field_valid?,
        filter_valid?: filter_valid?,
        scope_satisfied?: scope_satisfied?,
        version_valid?: version_valid?,
        fingerprint_valid?: fingerprint_valid?
      }

      consumer = consumer_domain(dimensions)

      Map.put(
        dimensions,
        :verification,
        ContractVerification.verify(provider_domain(), consumer)
      )
    end
  end

  defp invariants do
    [
      {"compatibility_result_is_complete", &compatibility_result_is_complete/1},
      {"incompatible_dimensions_have_specific_diagnostics", &specific_diagnostics/1}
    ]
  end

  defp compatibility_result_is_complete(state) do
    expected_compatible? =
      Enum.all?(
        Map.take(state, [
          :field_valid?,
          :filter_valid?,
          :scope_satisfied?,
          :version_valid?,
          :fingerprint_valid?
        ]),
        fn {_dimension, valid?} -> valid? end
      )

    actual_compatible? = match?({:ok, _report}, state.verification)

    if actual_compatible? == expected_compatible?,
      do: :ok,
      else: {:error, %{expected_compatible?: expected_compatible?, result: state.verification}}
  end

  defp specific_diagnostics(%{verification: {:ok, _report}}), do: :ok

  defp specific_diagnostics(%{verification: {:error, report}} = state) do
    codes = MapSet.new(report.errors, &Map.fetch!(&1, :code))

    expected_codes =
      [
        {not state.field_valid?, :missing_field},
        {not state.filter_valid?, :missing_filter},
        {not state.scope_satisfied?, :unsatisfied_required_filter},
        {not state.version_valid?, :incompatible_contract_version},
        {not state.fingerprint_valid?, :provider_fingerprint_changed}
      ]
      |> Enum.filter(fn {expected?, _code} -> expected? end)
      |> Enum.map(fn {_expected?, code} -> code end)
      |> MapSet.new()

    missing = MapSet.difference(expected_codes, codes) |> MapSet.to_list()

    if missing == [], do: :ok, else: {:error, %{missing_diagnostic_codes: missing}}
  end

  defp provider_domain do
    base_domain()
    |> update_in([:source, :fields], &(&1 ++ [:tenant_id]))
    |> put_in([:source, :columns, :tenant_id], %{type: :integer})
    |> Map.merge(%{
      name: :billing,
      domain_version: "1.0.0",
      domain_fingerprint: "sha256:provider",
      published_views: %{
        invoice_summary_v1: %{
          version: "1.0.0",
          compatibility: "~> 1.0",
          stable: true,
          database_name: "reporting.invoice_summary",
          kind: :view,
          query: fn selecto -> selecto end,
          columns: %{
            invoice_id: %{type: :integer},
            status: %{type: :string},
            balance_due: %{type: :decimal}
          },
          filters: [:status_picker],
          required_filters: [:tenant_id],
          fingerprint: "sha256:surface"
        }
      }
    })
  end

  defp consumer_domain(state) do
    base_domain()
    |> Map.merge(%{
      name: :registration,
      domain_dependencies: [
        %{
          provider: :billing,
          contract: :invoice_summary_v1,
          accepts: if(state.version_valid?, do: "~> 1.0", else: "~> 2.0"),
          expected_fingerprint:
            if(state.fingerprint_valid?, do: "sha256:surface", else: "sha256:old"),
          uses: %{
            fields: [if(state.field_valid?, do: :invoice_id, else: :missing_field)],
            filters: [if(state.filter_valid?, do: :status_picker, else: :missing_filter)],
            query_members: []
          },
          satisfies: if(state.scope_satisfied?, do: [:tenant_id], else: [])
        }
      ]
    })
  end

  defp base_domain do
    %{
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :status],
        columns: %{
          id: %{type: :integer},
          status: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      filters: %{
        status_picker: %{field: :status, type: :string}
      },
      required_filters: [{"status", "open"}]
    }
  end
end
