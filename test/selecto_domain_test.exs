defmodule Selecto.DomainTest do
  use ExUnit.Case, async: true

  alias Selecto.Domain

  describe "normalize/1" do
    test "normalizes a minimal query domain and infers schema_version" do
      {:ok, normalized, diagnostics} = Domain.normalize(minimal_query_domain())

      assert normalized.schema_version == 1
      assert normalized.domain.schema_version == 1
      assert normalized.authored_domain == minimal_query_domain()
      assert normalized.source.source_table == "orders"
      assert normalized.schemas == %{}
      assert normalized.joins == %{}

      assert diagnostics.schema_version == 1
      assert diagnostics.schema_version_inferred
      assert :source in diagnostics.canonical_sections
      assert :schemas in diagnostics.canonical_sections
      assert :joins in diagnostics.canonical_sections
      assert diagnostics.projection_sections == []
      assert diagnostics.proposed_sections == []
      assert diagnostics.unknown_sections == []
      assert :schema_version_inferred in warning_codes(diagnostics)
    end

    test "classifies generated-style query sections without making them unknown" do
      {:ok, normalized, diagnostics} = Domain.normalize(generated_style_domain())

      assert normalized.query.default_selected == [:id, :name]
      assert normalized.query.filters["name"].type == :string
      assert normalized.projection.custom_columns["name_upper"].type == :string
      assert normalized.projection.pagination.default_limit == 50
      assert normalized.projection.retarget == %{default_target: :customers}

      assert :custom_columns in diagnostics.projection_sections
      assert :pagination in diagnostics.projection_sections
      assert :retarget in diagnostics.projection_sections
      assert :subfilters in diagnostics.projection_sections
      assert :window_functions in diagnostics.projection_sections
      assert diagnostics.unknown_sections == [:pivot]
      assert :projection_sections in warning_codes(diagnostics)
      assert :unknown_sections in warning_codes(diagnostics)
    end

    test "exposes proposed writes and flags old write-like top-level sections as unknown" do
      {:ok, normalized, diagnostics} = Domain.normalize(write_style_domain())

      assert normalized.writes.operations.insert.fields == [:status, :total]
      assert normalized.writes.fields.status.required_on_insert

      assert :writes in diagnostics.proposed_sections

      assert diagnostics.unknown_sections == [
               :required_on_insert,
               :soft_delete_field,
               :transitions,
               :writable
             ]

      assert :proposed_sections in warning_codes(diagnostics)
      assert :unknown_sections in warning_codes(diagnostics)
    end

    test "keeps current detail_actions as a canonical compatibility section" do
      {:ok, normalized, diagnostics} = Domain.normalize(detail_action_domain())

      assert normalized.detail_actions.profile.type == :modal
      assert normalized.detail_actions.docs.type == :iframe_modal
      assert normalized.detail_actions.external.type == :external_link
      assert normalized.detail_actions.live.type == :live_component

      assert :detail_actions in diagnostics.canonical_sections
      assert diagnostics.unknown_sections == []
    end

    test "keeps query_members in the query registry" do
      {:ok, normalized, diagnostics} = Domain.normalize(query_member_domain())

      assert normalized.query.query_members.ctes.delivered_orders.columns == ["id", "total"]
      assert normalized.query.query_members.values.status_lookup.as == "status_lookup"
      assert normalized.query.query_members.subqueries.high_value_orders.type == :inner
      assert normalized.query.query_members.laterals.expand_tags.as == "tag_rows"
      assert normalized.query.query_members.unnests.tags.ordinality == "tag_position"

      assert :query_members in diagnostics.canonical_sections
      assert diagnostics.unknown_sections == []
    end

    test "classifies future sections as proposed and leaves truly unknown sections visible" do
      {:ok, normalized, diagnostics} = Domain.normalize(future_section_domain())

      refute diagnostics.schema_version_inferred
      assert normalized.schema_version == 2
      assert normalized.capabilities["invoice.view"].operations == [:select, :detail]
      assert normalized.actions.approve_invoice.type == :transition
      assert normalized.source_relationships.customer.target_domain == :customers
      assert normalized.choice_sources.customer_choices.domain == :customers

      assert :actions in diagnostics.proposed_sections
      assert :capabilities in diagnostics.proposed_sections
      assert :choice_sources in diagnostics.proposed_sections
      assert :source_relationships in diagnostics.proposed_sections
      assert :future_runtime_metadata in diagnostics.unknown_sections
      assert :proposed_sections in warning_codes(diagnostics)
      assert :unknown_sections in warning_codes(diagnostics)
    end

    test "returns diagnostics for non-map input" do
      assert {:error, diagnostics} = Domain.normalize(:not_a_domain)

      assert [%{code: :invalid_domain}] = diagnostics.errors
    end
  end

  defp warning_codes(diagnostics) do
    Enum.map(diagnostics.warnings, & &1.code)
  end

  defp minimal_query_domain do
    %{
      name: "Orders",
      source: %{
        source_table: "orders",
        primary_key: :id,
        fields: [:id, :status, :total],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          status: %{type: :string},
          total: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp generated_style_domain do
    minimal_query_domain()
    |> Map.merge(%{
      default_selected: [:id, :name],
      filters: %{
        "name" => %{name: "Name", type: :string}
      },
      custom_columns: %{
        "name_upper" => %{name: "Upper Name", select: {:func, "upper", [:name]}, type: :string}
      },
      subfilters: %{},
      window_functions: %{},
      pagination: %{
        default_limit: 50,
        max_limit: 1000
      },
      retarget: %{
        default_target: :customers
      },
      pivot: %{}
    })
  end

  defp write_style_domain do
    minimal_query_domain()
    |> Map.merge(%{
      writable: [:status, :total],
      required_on_insert: [:status],
      soft_delete_field: :deleted_at,
      transitions: %{
        submitted: %{to: [:paid, :cancelled]}
      },
      writes: %{
        operations: %{
          insert: %{fields: [:status, :total]},
          update: %{fields: [:status]}
        },
        fields: %{
          status: %{writable: true, required_on_insert: true},
          total: %{writable: true}
        },
        relationships: %{},
        validations: [],
        constraints: []
      }
    })
  end

  defp detail_action_domain do
    minimal_query_domain()
    |> Map.put(:detail_actions, %{
      profile: %{
        name: "Profile",
        type: :modal,
        required_fields: [:id],
        payload: %{title: "Order"}
      },
      docs: %{
        name: "Docs",
        type: :iframe_modal,
        required_fields: [:id],
        payload: %{url_template: "/orders/{{id}}/docs"}
      },
      external: %{
        name: "External",
        type: :external_link,
        required_fields: [:id],
        payload: %{url_template: "https://example.test/orders/{{id}}"}
      },
      live: %{
        name: "Live",
        type: :live_component,
        required_fields: [:id],
        payload: %{module: ExampleLiveComponent}
      }
    })
  end

  defp query_member_domain do
    minimal_query_domain()
    |> Map.put(:query_members, %{
      ctes: %{
        delivered_orders: %{
          query: fn selecto -> selecto end,
          columns: ["id", "total"],
          join: [owner_key: :id, related_key: :id]
        }
      },
      values: %{
        status_lookup: %{
          rows: [["delivered", "Delivered"]],
          columns: ["status", "label"],
          as: "status_lookup"
        }
      },
      subqueries: %{
        high_value_orders: %{
          query: fn selecto -> selecto end,
          type: :inner,
          on: [%{left: "id", right: "order_id"}]
        }
      },
      laterals: %{
        expand_tags: %{
          source: {:unnest, "\"selecto_root\".\"tags\""},
          join_type: :left,
          as: "tag_rows"
        }
      },
      unnests: %{
        tags: %{
          array_field: "tags",
          as: "tag",
          ordinality: "tag_position"
        }
      }
    })
  end

  defp future_section_domain do
    minimal_query_domain()
    |> Map.merge(%{
      schema_version: 2,
      capabilities: %{
        "invoice.view" => %{operations: [:select, :detail]}
      },
      actions: %{
        approve_invoice: %{
          type: :transition,
          transition: :approve,
          required_fields: [:id]
        }
      },
      source_relationships: %{
        customer: %{
          target_domain: :customers,
          source_field: :customer_id,
          target_field: :id
        }
      },
      choice_sources: %{
        customer_choices: %{
          domain: :customers,
          value_field: :id,
          label_field: :name
        }
      },
      future_runtime_metadata: %{mode: :experimental}
    })
  end
end
