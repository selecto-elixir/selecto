defmodule Selecto.DomainTest do
  use ExUnit.Case, async: true

  alias Selecto.Domain

  defmodule ComposeExtension do
    @behaviour Selecto.Extension

    @impl true
    def merge_domain(domain, opts) do
      filter_id = Keyword.get(opts, :filter_id, "extension_filter")

      Selecto.Extensions.deep_merge(domain, %{
        filters: %{
          filter_id => %{field: :status, source: :extension}
        }
      })
    end
  end

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

      assert :unsupported_schema_version in warning_codes(diagnostics)
      assert :actions in diagnostics.proposed_sections
      assert :capabilities in diagnostics.proposed_sections
      assert :choice_sources in diagnostics.proposed_sections
      assert :source_relationships in diagnostics.proposed_sections
      assert :future_runtime_metadata in diagnostics.unknown_sections
      assert :proposed_sections in warning_codes(diagnostics)
      assert :unknown_sections in warning_codes(diagnostics)
    end

    test "normalizes field-level choice source shorthand into canonical registries" do
      domain = choice_source_shorthand_domain()

      assert {:ok, normalized, diagnostics} = Domain.normalize(domain)

      assert normalized.authored_domain == domain
      assert normalized.source.columns.customer_id.choice_source == :customer_choices

      assert normalized.source.columns.customer_id.reference == %{
               choice_source: :customer_choices,
               value_source: "customers.id",
               caption_source: "customers.name"
             }

      assert normalized.source_relationships.customer == %{
               target_domain: :customers,
               source_field: :customer_id,
               target_field: "id",
               source_path: "customers",
               virtual_join: [
                 %{working_field: :customer_id, source_field: "customers.id", required: true}
               ],
               filters: [{:eq, "customers.tenant_id", {:context, :tenant_id}}]
             }

      assert normalized.choice_sources.customer_choices == %{
               domain: :customers,
               source_relationship: :customer,
               value_source: "customers.id",
               caption_source: "customers.name",
               value_field: "id",
               label_field: "name",
               source_path: "customers",
               filters: [{:eq, "customers.active", true}],
               order_by: ["customers.name"],
               presentation: %{control: :select}
             }

      assert diagnostics.proposed_sections == []
      assert {:ok, _normalized, _diagnostics} = Domain.validate(domain)
    end

    test "generates deterministic string ids for choice source shorthand" do
      domain =
        choice_source_shorthand_domain()
        |> update_in([:source, :columns, :customer_id, :choice_source], fn shorthand ->
          shorthand
          |> Map.delete(:id)
          |> update_in([:source_relationship], &Map.delete(&1, :id))
        end)

      assert {:ok, normalized, _diagnostics} = Domain.normalize(domain)

      assert normalized.source.columns.customer_id.choice_source == "customer_id_choice_source"

      assert Map.has_key?(normalized.choice_sources, "customer_id_choice_source")
      assert Map.has_key?(normalized.source_relationships, "customer_id_source_relationship")

      assert normalized.choice_sources["customer_id_choice_source"].source_relationship ==
               "customer_id_source_relationship"

      assert {:ok, _normalized, _diagnostics} = Domain.validate(domain)
    end

    test "returns diagnostics for non-map input" do
      assert {:error, diagnostics} = Domain.normalize(:not_a_domain)

      assert [%{code: :invalid_domain}] = diagnostics.errors
    end

    test "warns on invalid schema_version and uses the current version" do
      domain = Map.put(minimal_query_domain(), :schema_version, "draft")

      {:ok, normalized, diagnostics} = Domain.normalize(domain)

      assert normalized.schema_version == 1
      refute diagnostics.schema_version_inferred

      assert %{
               code: :invalid_schema_version,
               value: "draft",
               schema_version: 1
             } = warning_for(diagnostics, :invalid_schema_version)
    end

    test "warns on malformed current section shapes without failing normalization" do
      domain =
        minimal_query_domain()
        |> Map.merge(%{
          name: %{bad: :name},
          source: :orders,
          schemas: [],
          joins: "bad",
          filters: [],
          default_selected: :id,
          custom_columns: [],
          extensions: %{not: :a_list}
        })

      {:ok, normalized, diagnostics} = Domain.normalize(domain)

      assert normalized.source == :orders

      assert invalid_shape_sections(diagnostics) == [
               :custom_columns,
               :default_selected,
               :extensions,
               :filters,
               :joins,
               :name,
               :schemas,
               :source
             ]
    end

    test "warns on malformed proposed section shapes without deep validation" do
      domain =
        minimal_query_domain()
        |> Map.merge(%{
          writes: [],
          actions: [],
          capabilities: [],
          source_relationships: [],
          choice_sources: []
        })

      {:ok, normalized, diagnostics} = Domain.normalize(domain)

      assert normalized.writes == []

      assert invalid_shape_sections(diagnostics) == [
               :actions,
               :capabilities,
               :choice_sources,
               :source_relationships,
               :writes
             ]
    end
  end

  describe "compose/2" do
    test "deep-merges overlays with explicit list semantics" do
      base =
        minimal_query_domain()
        |> put_in([:source, :redact_fields], [:internal_notes])
        |> put_in([:filters], %{
          "status" => %{field: :status, label: "Status"}
        })

      overlay_one = %{
        source: %{
          columns: %{
            total: %{label: "Total"}
          },
          redact_fields: [:tenant_secret]
        },
        filters: %{
          "status" => %{label: "Order Status"}
        }
      }

      overlay_two = %{
        source: %{
          columns: %{
            total: %{format: :currency}
          },
          redact_fields: [:tenant_secret, :audit_token]
        },
        required_selected: [:id, :status]
      }

      assert {:ok, normalized, diagnostics} = Domain.compose(base, [overlay_one, overlay_two])

      assert normalized.source.columns.total == %{
               type: :decimal,
               label: "Total",
               format: :currency
             }

      assert normalized.source.redact_fields == [:internal_notes, :tenant_secret, :audit_token]
      assert normalized.query.filters["status"] == %{field: :status, label: "Order Status"}
      assert normalized.query.required_selected == [:id, :status]

      refute :domain_composition_collision in warning_codes(diagnostics)
    end

    test "applies extension merge_domain callbacks after overlay composition" do
      overlay = %{
        extensions: [
          {ComposeExtension, filter_id: "extension_status"}
        ]
      }

      assert {:ok, normalized, _diagnostics} = Domain.compose(minimal_query_domain(), overlay)

      assert normalized.extensions == [{ComposeExtension, [filter_id: "extension_status"]}]

      assert normalized.query.filters["extension_status"] == %{
               field: :status,
               source: :extension
             }
    end

    test "warns when overlays update existing reference registry entries" do
      base =
        minimal_query_domain()
        |> Map.put(:choice_sources, %{
          customer_choices: %{domain: :customers, value_field: :id, label_field: :name}
        })

      overlay = %{
        choice_sources: %{
          "customer_choices" => %{presentation: %{control: :autocomplete}}
        }
      }

      assert {:ok, normalized, diagnostics} = Domain.compose(base, overlay)

      assert normalized.choice_sources.customer_choices == %{
               domain: :customers,
               value_field: :id,
               label_field: :name,
               presentation: %{control: :autocomplete}
             }

      assert %{
               code: :domain_composition_collision,
               section: :choice_sources,
               key: "customer_choices",
               overlay_index: 0
             } = warning_for(diagnostics, :domain_composition_collision)
    end

    test "returns diagnostics for invalid overlays" do
      assert {:error, diagnostics} = Domain.compose(minimal_query_domain(), [:not_an_overlay])

      assert [
               %{
                 code: :invalid_domain_overlay,
                 overlay_index: 0,
                 actual: :atom
               }
             ] = diagnostics.errors
    end
  end

  describe "describe/1" do
    test "returns structured inspection output for normalized domains" do
      domain =
        choice_source_shorthand_domain()
        |> Map.put(:capabilities, %{
          "customer.choose" => %{operations: [:choice_source]}
        })
        |> Map.put(:actions, %{
          choose_customer: %{type: :choice_source, capability: "customer.choose"}
        })
        |> Map.put(:writes, %{
          fields: %{customer_id: %{updatable: true}},
          transitions: %{status: %{"open" => ["closed"]}}
        })

      assert {:ok, normalized, _diagnostics} = Domain.normalize(domain)
      assert {:ok, inspection, diagnostics} = Domain.describe(normalized)

      assert inspection.schema_version == 1
      assert inspection.name == "Orders"
      assert inspection.projections == [:query, :write, :ui, :api, :query_contract]
      assert inspection.diagnostics.error_count == 0
      assert inspection.counts.source_fields == 4
      assert inspection.counts.choice_sources == 1
      assert inspection.counts.source_relationships == 1
      assert inspection.counts.field_choice_bindings == 1
      assert inspection.counts.writes.fields == 1
      assert inspection.counts.actions == 1
      assert inspection.counts.capabilities == 1

      assert inspection.registries.source_fields == ["customer_id", "id", "status", "total"]
      assert inspection.registries.choice_sources == [:customer_choices]
      assert inspection.writes.transitions == [:status]

      assert [
               %{
                 id: :customer,
                 target_domain: :customers,
                 source_field: :customer_id,
                 target_field: "id",
                 source_path: "customers",
                 virtual_join_count: 1,
                 filters_count: 1
               }
             ] = inspection.source_relationships

      assert [
               %{
                 id: :customer_choices,
                 domain: :customers,
                 source_relationship: :customer,
                 value_field: "id",
                 label_field: "name",
                 source_path: "customers",
                 filters_count: 1,
                 order_by_count: 1,
                 presentation: %{control: :select}
               }
             ] = inspection.choice_sources

      assert [
               %{
                 field: :customer_id,
                 choice_source: :customer_choices,
                 compact?: true,
                 reference?: true,
                 path: [:source, :columns, :customer_id]
               }
             ] = inspection.field_choice_bindings

      assert [
               %{id: "customer.choose", operations: [:choice_source], action: nil}
             ] = inspection.capabilities

      assert [
               %{id: :choose_customer, type: :choice_source, capability: "customer.choose"}
             ] = inspection.actions

      assert diagnostics.schema_version == 1
    end

    test "accepts authored domains and includes normalization diagnostics" do
      assert {:ok, inspection, diagnostics} = Domain.describe(generated_style_domain())

      assert inspection.registries.filters == ["name"]
      assert inspection.registries.custom_columns == ["name_upper"]
      assert inspection.counts.query_members == 0
      assert :schema_version_inferred in inspection.diagnostics.warning_codes
      assert :unknown_sections in inspection.diagnostics.warning_codes
      assert diagnostics.schema_version_inferred
    end

    test "returns diagnostics for invalid inspection inputs" do
      assert {:error, diagnostics} = Domain.describe(:not_a_domain)

      assert [%{code: :invalid_domain}] = diagnostics.errors
    end
  end

  describe "project/2" do
    test "projects a query-facing domain without unknown or write sections" do
      {:ok, normalized, _diagnostics} = Domain.normalize(generated_style_domain())

      projection = Domain.project(normalized, :query)

      assert projection.schema_version == 1
      assert projection.name == "Orders"
      assert projection.source.source_table == "orders"
      assert projection.default_selected == [:id, :name]
      assert projection.filters["name"].type == :string
      assert projection.custom_columns["name_upper"].type == :string
      assert projection.pagination.default_limit == 50
      assert projection.retarget == %{default_target: :customers}

      refute Map.has_key?(projection, :pivot)
      refute Map.has_key?(projection, :writes)
      refute Map.has_key?(projection, :actions)
      refute Map.has_key?(projection, :detail_actions)
    end

    test "projects a write-facing domain without old top-level write keys" do
      {:ok, normalized, _diagnostics} = Domain.normalize(write_style_domain())

      projection = Domain.project(normalized, :write)

      assert projection.schema_version == 1
      assert projection.columns.status.type == :string
      assert projection.writes.operations.insert.fields == [:status, :total]
      assert projection.actions == %{}
      assert projection.capabilities == %{}
      assert projection.source_relationships == %{}
      assert projection.choice_sources == %{}

      refute Map.has_key?(projection, :writable)
      refute Map.has_key?(projection, :required_on_insert)
      refute Map.has_key?(projection, :soft_delete_field)
      refute Map.has_key?(projection, :transitions)
      refute Map.has_key?(projection, :filters)
      refute Map.has_key?(projection, :detail_actions)
    end

    test "projects a ui-facing domain with display defaults and actions" do
      {:ok, normalized, _diagnostics} =
        detail_action_domain()
        |> Map.merge(future_sections())
        |> Domain.normalize()

      projection = Domain.project(normalized, :ui)

      assert projection.detail_actions.profile.type == :modal
      assert projection.actions.approve_invoice.type == :transition
      assert projection.capabilities["invoice.view"].operations == [:select, :detail]
      assert projection.choice_sources.customer_choices.domain == :customers
      assert projection.filters == %{}

      refute Map.has_key?(projection, :writes)
      refute Map.has_key?(projection, :query_members)
      refute Map.has_key?(projection, :source_relationships)
      refute Map.has_key?(projection, :future_runtime_metadata)
    end

    test "projects an api-facing domain with read, write, action, and reference sections" do
      {:ok, normalized, _diagnostics} =
        write_style_domain()
        |> Map.merge(future_sections())
        |> Domain.normalize()

      projection = Domain.project(normalized, :api)

      assert projection.filters == %{}
      assert projection.query_members == %{}
      assert projection.columns.status.type == :string
      assert projection.writes.operations.insert.fields == [:status, :total]
      assert projection.actions.approve_invoice.type == :transition
      assert projection.capabilities["invoice.view"].operations == [:select, :detail]
      assert projection.source_relationships.customer.target_domain == :customers
      assert projection.choice_sources.customer_choices.domain == :customers

      refute Map.has_key?(projection, :writable)
      refute Map.has_key?(projection, :future_runtime_metadata)
    end

    test "projects a constrained query contract for tools and AI" do
      {:ok, normalized, _diagnostics} = Domain.normalize(query_contract_domain())

      projection = Domain.project(normalized, :query_contract)

      assert projection.schema_version == 1
      assert projection.name == "Orders"
      assert projection.projection == :query_contract
      assert projection.source == %{source_table: "orders", primary_key: :id}
      assert projection.defaults.default_selected == [:id, "customers.name"]

      customer_id_field = Enum.find(projection.fields, &(&1.id == "customer_id"))

      assert %{
               id: "customer_id",
               source: :source,
               relation: :source,
               field: "customer_id",
               type: :integer,
               label: "Customer",
               choice_source: :customer_choices,
               detail_selectable: true,
               filterable: true,
               sortable: true,
               groupable: true,
               aggregatable: true,
               aggregate_functions: [:count, :count_distinct, :sum, :avg, :min, :max]
             } = customer_id_field

      assert :between in customer_id_field.comparators

      assert %{
               id: "customers.name",
               source: :schema,
               relation: :customers,
               field: "name",
               type: :string,
               detail_selectable: true,
               filterable: true,
               sortable: true,
               groupable: true,
               aggregatable: false,
               aggregate_functions: []
             } = customers_name_field = Enum.find(projection.fields, &(&1.id == "customers.name"))

      assert :contains in customers_name_field.comparators

      assert %{
               id: "inserted_at",
               type: :utc_datetime,
               filterable: true,
               sortable: true,
               groupable: true,
               aggregatable: false,
               aggregate_functions: []
             } = inserted_at_field = Enum.find(projection.fields, &(&1.id == "inserted_at"))

      assert :between in inserted_at_field.comparators

      assert %{
               id: "status_label",
               source: :custom_column,
               relation: nil,
               type: :string,
               detail_selectable: true,
               filterable: false,
               sortable: true,
               groupable: true,
               aggregatable: false,
               comparators: [],
               aggregate_functions: []
             } = Enum.find(projection.fields, &(&1.id == "status_label"))

      assert [
               %{
                 id: "customer",
                 path: ["customer"],
                 parent: :source,
                 target_schema: :customers,
                 type: :left,
                 fields: ["id", "name"],
                 nested_count: 0
               }
             ] = projection.joins

      assert %{
               id: "recent",
               field: nil,
               type: :boolean,
               virtual?: true,
               comparators: []
             } = Enum.find(projection.filters, &(&1.id == "recent"))

      assert %{
               id: "status_filter",
               field: "status",
               type: :string,
               capability: "order.filter",
               virtual?: false
             } = status_filter = Enum.find(projection.filters, &(&1.id == "status_filter"))

      assert :contains in status_filter.comparators

      assert [
               %{
                 id: "similarity",
                 kind: :scalar,
                 sql_name: "public.similarity",
                 allowed_in: [:select, :order_by],
                 capability: "order.rank",
                 args: [
                   %{name: :left, type: :string, source: :selector},
                   %{name: :right, type: :string, source: :value}
                 ]
               }
             ] = projection.functions

      assert [
               %{id: :recent_orders, columns: ["id", "status"], join?: true}
             ] = projection.query_members.ctes

      assert [
               %{
                 id: :status_lookup,
                 columns: ["status", "label"],
                 alias: "status_lookup",
                 rows_count: 1,
                 capability: "order.member"
               }
             ] = projection.query_members.values

      assert [
               %{
                 id: "order_rollup",
                 database_name: "reporting.order_rollup",
                 kind: :view,
                 columns: [%{id: "order_id", type: :integer}],
                 capability: "order.view"
               }
             ] = projection.published_views

      assert [
               %{
                 id: :customer_choices,
                 domain: :customers,
                 source_relationship: :customer,
                 value_field: "id",
                 label_field: "name",
                 capability: "customer.choose"
               }
             ] = projection.choice_sources

      assert [
               %{
                 id: :customer,
                 target_domain: :customers,
                 source_field: "customer_id",
                 target_field: "id"
               }
             ] = projection.source_relationships

      assert [
               %{
                 field: "customer_id",
                 choice_source: :customer_choices,
                 compact?: true,
                 reference?: false
               }
             ] = projection.field_choice_bindings

      assert projection.capability_ids == [
               "customer.choose",
               "order.filter",
               "order.member",
               "order.rank",
               "order.view"
             ]

      refute Map.has_key?(projection, :writes)
      refute Map.has_key?(projection, :actions)
      refute Map.has_key?(projection, :detail_actions)
      refute inspect(projection) =~ "#Function<"
    end

    test "raises for unknown projections and raw domains" do
      {:ok, normalized, _diagnostics} = Domain.normalize(minimal_query_domain())

      assert_raise ArgumentError, ~r/unknown Selecto domain projection :export/, fn ->
        Domain.project(normalized, :export)
      end

      assert_raise ArgumentError, ~r/expected a normalized Selecto domain/, fn ->
        Domain.project(minimal_query_domain(), :query)
      end
    end
  end

  describe "query_contract/1" do
    test "normalizes authored domains and returns query contract diagnostics" do
      assert {:ok, contract, diagnostics} = Domain.query_contract(query_contract_domain())

      assert contract.projection == :query_contract
      assert contract.source == %{source_table: "orders", primary_key: :id}
      assert Enum.any?(contract.fields, &(&1.id == "customer_id"))
      assert "order.filter" in contract.capability_ids
      assert diagnostics.schema_version_inferred
      assert :schema_version_inferred in warning_codes(diagnostics)
    end

    test "accepts normalized domains directly" do
      {:ok, normalized, _normalize_diagnostics} = Domain.normalize(query_contract_domain())

      assert {:ok, contract, diagnostics} = Domain.query_contract(normalized)

      assert contract.projection == :query_contract
      assert Enum.any?(contract.choice_sources, &(&1.id == :customer_choices))
      refute diagnostics.schema_version_inferred
      refute :schema_version_inferred in warning_codes(diagnostics)
    end

    test "returns diagnostics for invalid query contract inputs" do
      assert {:error, diagnostics} = Domain.query_contract(:not_a_domain)

      assert [%{code: :invalid_domain}] = diagnostics.errors
    end
  end

  defp warning_codes(diagnostics) do
    Enum.map(diagnostics.warnings, & &1.code)
  end

  defp warning_for(diagnostics, code) do
    Enum.find(diagnostics.warnings, &(&1.code == code))
  end

  defp invalid_shape_sections(diagnostics) do
    diagnostics.warnings
    |> Enum.filter(&(&1.code == :invalid_section_shape))
    |> Enum.map(& &1.section)
    |> Enum.sort()
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
      domain_data: %{
        source: :fixture
      },
      extensions: [
        {ExampleExtension, []}
      ],
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
      columns: %{
        status: %{type: :string},
        total: %{type: :decimal}
      },
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

  defp query_contract_domain do
    minimal_query_domain()
    |> put_in([:source, :fields], [:id, :status, :total, :customer_id, :inserted_at])
    |> put_in([:source, :columns, :customer_id], %{
      type: :integer,
      label: "Customer",
      choice_source: :customer_choices
    })
    |> put_in([:source, :columns, :inserted_at], %{type: :utc_datetime, label: "Inserted At"})
    |> put_in([:source, :associations, :customer], %{
      queryable: :customers,
      owner_key: :customer_id,
      related_key: :id
    })
    |> Map.put(:schemas, %{
      customers: %{
        source_table: "customers",
        primary_key: :id,
        fields: [:id, :name],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string}
        },
        associations: %{}
      }
    })
    |> Map.merge(%{
      joins: %{customer: %{type: :left}},
      default_selected: [:id, "customers.name"],
      filters: %{
        "recent" => %{
          type: :boolean,
          label: "Recent"
        },
        "status_filter" => %{
          field: :status,
          type: :string,
          label: "Status",
          capability: "order.filter"
        }
      },
      functions: %{
        "similarity" => %{
          kind: :scalar,
          sql_name: "public.similarity",
          allowed_in: [:select, :order_by],
          capability: "order.rank",
          args: [
            %{name: :left, type: :string, source: :selector},
            %{name: :right, type: :string, source: :value}
          ],
          returns: :float
        }
      },
      query_members: %{
        ctes: %{
          recent_orders: %{
            query: fn selecto -> selecto end,
            columns: ["id", "status"],
            join: [owner_key: :id, related_key: :id]
          }
        },
        values: %{
          status_lookup: %{
            rows: [["open", "Open"]],
            columns: ["status", "label"],
            as: "status_lookup",
            capability: "order.member"
          }
        }
      },
      published_views: %{
        "order_rollup" => %{
          database_name: "reporting.order_rollup",
          kind: :view,
          query: fn selecto -> selecto end,
          columns: %{order_id: %{type: :integer}},
          capability: "order.view"
        }
      },
      custom_columns: %{
        "status_label" => %{select: {:field, :status}, type: :string}
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
          label_field: :name,
          source_relationship: :customer,
          capability: "customer.choose"
        }
      },
      capabilities: %{
        "customer.choose" => %{operations: [:choice_source]},
        "order.filter" => %{operations: [:filter]},
        "order.member" => %{operations: [:query_member]},
        "order.rank" => %{operations: [:select]},
        "order.view" => %{operations: [:select]}
      }
    })
  end

  defp future_section_domain do
    minimal_query_domain()
    |> Map.merge(future_sections())
  end

  defp choice_source_shorthand_domain do
    minimal_query_domain()
    |> put_in([:source, :fields], [:id, :status, :total, :customer_id])
    |> put_in([:source, :columns, :customer_id], %{
      type: :integer,
      choice_source: %{
        id: :customer_choices,
        domain: :customers,
        source_relationship: %{
          id: :customer,
          virtual_join: [
            %{working_field: :customer_id, source_field: "customers.id", required: true}
          ],
          filters: [{:eq, "customers.tenant_id", {:context, :tenant_id}}]
        },
        value_source: "customers.id",
        caption_source: "customers.name",
        filters: [{:eq, "customers.active", true}],
        order_by: ["customers.name"],
        presentation: :select
      }
    })
  end

  defp future_sections do
    %{
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
    }
  end
end
