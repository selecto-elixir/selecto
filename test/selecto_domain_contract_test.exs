defmodule Selecto.DomainContractTest do
  use ExUnit.Case, async: true

  alias Selecto.Domain
  alias Selecto.Domain.Contract
  alias Selecto.DomainValidator

  describe "validate/1" do
    test "accepts a minimal normalized domain" do
      {:ok, normalized, diagnostics} = Domain.normalize(valid_domain())

      assert diagnostics.errors == []
      assert Contract.validate(normalized) == :ok
      assert {:ok, ^normalized, _diagnostics} = Domain.validate(valid_domain())
    end

    test "accepts atom and string keys in first-wave contract sections" do
      domain = %{
        "source" => %{
          "source_table" => "orders",
          "primary_key" => "id",
          "fields" => ["id", "status"],
          "columns" => %{
            "id" => %{"type" => "integer"},
            "status" => %{"type" => "string"}
          },
          "associations" => %{}
        },
        "schemas" => %{},
        "joins" => %{},
        "filters" => %{
          "status_filter" => %{"field" => "status", "type" => "string"}
        },
        "required_filters" => [{"status", "open"}]
      }

      assert {:ok, _normalized, _diagnostics} = Domain.validate(domain)
    end

    test "reports missing required sections as structured diagnostics" do
      assert {:error, diagnostics} = Domain.validate(%{source: valid_source()})

      assert %{
               code: :missing_required_section,
               section: :schemas,
               path: [:schemas]
             } = error_for(diagnostics, :missing_required_section)
    end

    test "validates source field and primary key references" do
      domain =
        valid_domain()
        |> put_in([:source, :fields], [:id, :status])
        |> put_in([:source, :primary_key], :missing_id)
        |> put_in([:source, :columns], %{id: %{type: :integer}})

      assert {:error, diagnostics} = Domain.validate(domain)

      assert %{
               code: :source_field_missing_column,
               relation: :source,
               field: :status,
               path: [:source, :columns, :status]
             } = error_for(diagnostics, :source_field_missing_column)

      assert %{
               code: :primary_key_not_found,
               relation: :source,
               field: :missing_id,
               path: [:source, :primary_key]
             } = error_for(diagnostics, :primary_key_not_found)
    end

    test "validates schema field column references" do
      domain =
        valid_domain()
        |> put_in([:schemas, :customers, :fields], [:id, :name, :email])
        |> put_in([:schemas, :customers, :columns], %{
          id: %{type: :integer},
          name: %{type: :string}
        })

      assert {:error, diagnostics} = Domain.validate(domain)

      assert %{
               code: :schema_field_missing_column,
               relation: :customers,
               field: :email,
               path: [:schemas, :customers, :columns, :email]
             } = error_for(diagnostics, :schema_field_missing_column)
    end

    test "validates join association and target schema references" do
      missing_association =
        valid_domain()
        |> put_in([:joins], %{bad_join: %{type: :left}})

      assert {:error, diagnostics} = Domain.validate(missing_association)

      assert %{
               code: :join_missing_association,
               parent: :source,
               join: :bad_join,
               path: [:joins, :bad_join]
             } = error_for(diagnostics, :join_missing_association)

      missing_target =
        valid_domain()
        |> put_in([:source, :associations, :customer, :queryable], :missing_customers)

      assert {:error, diagnostics} = Domain.validate(missing_target)

      assert %{
               code: :join_target_schema_not_found,
               join: :customer,
               schema: :missing_customers,
               path: [:joins, :customer]
             } = error_for(diagnostics, :join_target_schema_not_found)
    end

    test "validates filter field references without requiring filter ids to be fields" do
      domain =
        valid_domain()
        |> Map.merge(%{
          filters: %{
            "status_picker" => %{field: :missing_status, type: :string},
            "virtual_search" => %{type: :string}
          },
          required_filters: [
            {"missing_required", true},
            {:and, [{"status", "open"}]}
          ]
        })

      assert {:error, diagnostics} = Domain.validate(domain)

      errors = errors_for(diagnostics, :filter_field_not_found)

      assert Enum.any?(errors, &match?(%{field: :missing_status}, &1))
      assert Enum.any?(errors, &match?(%{field: "missing_required"}, &1))
      refute Enum.any?(errors, &match?(%{field: "status_picker"}, &1))
      refute Enum.any?(errors, &match?(%{field: "virtual_search"}, &1))
    end

    test "accepts write transition graphs for known fields" do
      domain =
        valid_domain()
        |> Map.put(:writes, %{
          transitions: %{
            status: %{
              "pending" => ["ready", "cancelled"],
              "ready" => [:complete, "cancelled"],
              complete: []
            }
          }
        })

      assert {:ok, _normalized, _diagnostics} = Domain.validate(domain)
    end

    test "validates write transition fields and state graph shape" do
      domain =
        valid_domain()
        |> Map.put(:writes, %{
          transitions: %{
            :missing_status => %{"pending" => ["ready"]},
            123 => %{"pending" => ["ready"]},
            status: %{
              "pending" => "ready",
              456 => ["ready"],
              "ready" => ["complete", 789]
            },
            customer_id: [:not, :a, :graph]
          }
        })

      assert {:error, diagnostics} = Domain.validate(domain)

      assert %{
               code: :transition_field_not_found,
               field: :missing_status,
               path: [:writes, :transitions, :missing_status]
             } = error_for(diagnostics, :transition_field_not_found)

      assert %{
               code: :invalid_transition_field,
               field: 123,
               path: [:writes, :transitions, 123]
             } = error_for(diagnostics, :invalid_transition_field)

      invalid_shapes = errors_for(diagnostics, :invalid_section_shape)

      assert Enum.any?(
               invalid_shapes,
               &match?(%{field: :customer_id, path: [:writes, :transitions, :customer_id]}, &1)
             )

      assert %{
               code: :invalid_transition_targets,
               path: [:writes, :transitions, :status, "pending"]
             } = error_for(diagnostics, :invalid_transition_targets)

      invalid_states = errors_for(diagnostics, :invalid_transition_state)

      assert Enum.any?(
               invalid_states,
               &match?(%{state: 456, path: [:writes, :transitions, :status, 456]}, &1)
             )

      assert Enum.any?(
               invalid_states,
               &match?(%{state: 789, path: [:writes, :transitions, :status, "ready", 1]}, &1)
             )
    end

    test "accepts capability catalog entries with atom and string operations" do
      domain =
        valid_domain()
        |> Map.put(:capabilities, %{
          "order.view" => %{
            label: "View orders",
            operations: [:select, "detail"],
            target: :order
          },
          order_export: %{
            operations: [:export],
            sensitivity: :high
          }
        })

      assert {:ok, _normalized, _diagnostics} = Domain.validate(domain)
    end

    test "validates capability catalog shape" do
      domain =
        valid_domain()
        |> Map.put(:capabilities, %{
          123 => %{operations: [:select]},
          "missing.operations" => %{label: "Missing operations"},
          "empty.operations" => %{operations: []},
          "bad.operations" => %{operations: :select},
          "bad.operation.member" => %{operations: [:select, 456]},
          "bad.config" => [:not, :a, :map]
        })

      assert {:error, diagnostics} = Domain.validate(domain)

      assert %{
               code: :invalid_capability_id,
               capability: 123,
               path: [:capabilities, 123]
             } = error_for(diagnostics, :invalid_capability_id)

      assert %{
               code: :capability_missing_operations,
               capability: "missing.operations",
               path: [:capabilities, "missing.operations", :operations]
             } = error_for(diagnostics, :capability_missing_operations)

      assert %{
               code: :capability_empty_operations,
               capability: "empty.operations",
               path: [:capabilities, "empty.operations", :operations]
             } = error_for(diagnostics, :capability_empty_operations)

      assert %{
               code: :invalid_capability_operations,
               capability: "bad.operations",
               path: [:capabilities, "bad.operations", :operations]
             } = error_for(diagnostics, :invalid_capability_operations)

      assert %{
               code: :invalid_capability_operation,
               capability: "bad.operation.member",
               operation: 456,
               path: [:capabilities, "bad.operation.member", :operations, 1]
             } = error_for(diagnostics, :invalid_capability_operation)

      invalid_shapes = errors_for(diagnostics, :invalid_section_shape)

      assert Enum.any?(
               invalid_shapes,
               &match?(%{capability: "bad.config", path: [:capabilities, "bad.config"]}, &1)
             )
    end

    test "accepts direct transition-backed row actions" do
      domain =
        valid_domain()
        |> Map.put(:capabilities, %{
          "order.complete" => %{operations: [:action], action: :complete_order}
        })
        |> Map.put(:writes, %{
          transitions: %{
            status: %{
              "pending" => ["ready", "cancelled"],
              "ready" => ["complete", "cancelled"],
              complete: []
            }
          }
        })
        |> Map.put(:actions, %{
          complete_order: %{
            target: :order,
            scope: :row,
            capability: "order.complete",
            transition: %{
              field: :status,
              from: "ready",
              to: :complete
            },
            execution: %{
              kind: :updato,
              operation: :update,
              set: %{status: :complete}
            }
          }
        })

      assert {:ok, _normalized, _diagnostics} = Domain.validate(domain)
    end

    test "validates direct transition-backed action references" do
      domain =
        valid_domain()
        |> Map.put(:capabilities, %{
          "order.complete" => %{operations: [:action], action: :complete_order}
        })
        |> Map.put(:writes, %{
          transitions: %{
            status: %{
              "pending" => ["ready"],
              "ready" => ["complete"]
            }
          }
        })
        |> Map.put(:actions, %{
          123 => %{
            transition: %{field: :status, from: "ready", to: "complete"}
          },
          bad_config: [:not, :a, :map],
          missing_capability: %{
            capability: "order.missing",
            transition: %{field: :status, from: "ready", to: "complete"}
          },
          bad_capability: %{
            capability: 456,
            transition: %{field: :status, from: "ready", to: "complete"}
          },
          missing_transition: %{
            type: :transition
          },
          bad_transition: %{
            transition: :approve
          },
          missing_field: %{
            transition: %{field: :missing_status, from: "ready", to: "complete"}
          },
          missing_edge: %{
            transition: %{field: :status, from: "pending", to: "complete"}
          },
          invalid_state: %{
            transition: %{field: :status, from: 789, to: "complete"}
          },
          bad_execution: %{
            transition: %{field: :status, from: "ready", to: "complete"},
            execution: %{kind: :other, operation: :delete, set: %{status: "cancelled"}}
          }
        })

      assert {:error, diagnostics} = Domain.validate(domain)

      assert %{
               code: :invalid_action_id,
               action: 123,
               path: [:actions, 123]
             } = error_for(diagnostics, :invalid_action_id)

      invalid_shapes = errors_for(diagnostics, :invalid_section_shape)

      assert Enum.any?(
               invalid_shapes,
               &match?(%{action: :bad_config, path: [:actions, :bad_config]}, &1)
             )

      assert %{
               code: :action_capability_not_found,
               action: :missing_capability,
               capability: "order.missing",
               path: [:actions, :missing_capability, :capability]
             } = error_for(diagnostics, :action_capability_not_found)

      assert %{
               code: :invalid_action_capability,
               action: :bad_capability,
               capability: 456,
               path: [:actions, :bad_capability, :capability]
             } = error_for(diagnostics, :invalid_action_capability)

      assert %{
               code: :action_missing_transition,
               action: :missing_transition,
               path: [:actions, :missing_transition, :transition]
             } = error_for(diagnostics, :action_missing_transition)

      assert %{
               code: :invalid_action_transition,
               action: :bad_transition,
               path: [:actions, :bad_transition, :transition]
             } = error_for(diagnostics, :invalid_action_transition)

      assert %{
               code: :action_transition_field_not_found,
               action: :missing_field,
               field: :missing_status,
               path: [:actions, :missing_field, :transition, :field]
             } = error_for(diagnostics, :action_transition_field_not_found)

      assert %{
               code: :action_transition_edge_not_found,
               action: :missing_edge,
               field: :status,
               from: "pending",
               to: "complete",
               path: [:actions, :missing_edge, :transition]
             } = error_for(diagnostics, :action_transition_edge_not_found)

      assert %{
               code: :invalid_action_transition_state,
               action: :invalid_state,
               state: 789,
               state_key: :from,
               path: [:actions, :invalid_state, :transition, :from]
             } = error_for(diagnostics, :invalid_action_transition_state)

      assert %{
               code: :invalid_action_execution_kind,
               action: :bad_execution,
               path: [:actions, :bad_execution, :execution, :kind]
             } = error_for(diagnostics, :invalid_action_execution_kind)

      assert %{
               code: :invalid_action_execution_operation,
               action: :bad_execution,
               path: [:actions, :bad_execution, :execution, :operation]
             } = error_for(diagnostics, :invalid_action_execution_operation)

      assert %{
               code: :action_execution_set_mismatch,
               action: :bad_execution,
               field: :status,
               to: "complete",
               path: [:actions, :bad_execution, :execution, :set]
             } = error_for(diagnostics, :action_execution_set_mismatch)
    end

    test "accepts source relationships and choice sources" do
      domain =
        valid_domain()
        |> Map.put(:capabilities, %{
          "customer.choose" => %{operations: [:choice_source]}
        })
        |> Map.put(:source_relationships, %{
          customer: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            source_path: "customers",
            virtual_join: [
              %{working_field: :customer_id, source_field: "customers.id", required: true}
            ],
            filters: [
              {:eq, "customers.active", true},
              ["not", ["eq", "customers.archived", true]]
            ]
          }
        })
        |> Map.put(:choice_sources, %{
          customer_choices: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            source_path: "customers",
            value_source: "customers.id",
            caption_source: "customers.name",
            description_source: "customers.description",
            filters: [
              {:eq, "customers.active", true},
              [
                "and",
                [
                  ["eq", "customers.tenant_id", {:context, :tenant_id}],
                  {:not, ["eq", "customers.archived", true]}
                ]
              ]
            ],
            order_by: ["customers.name", {"customers.id", :desc}],
            presentation: %{
              control: :autocomplete,
              mode: :searchable,
              cardinality: :one
            },
            source_relationship: :customer,
            capability: "customer.choose"
          }
        })
        |> put_in([:source, :columns, :customer_id, :reference], %{
          choice_source: :customer_choices,
          value_source: "customers.id",
          caption_source: "customers.name"
        })
        |> Map.put(:columns, %{
          customer_id: %{choice_source: :customer_choices}
        })

      assert {:ok, _normalized, _diagnostics} = Domain.validate(domain)
    end

    test "validates source relationship and choice source shapes" do
      domain =
        valid_domain()
        |> Map.put(:capabilities, %{
          "customer.choose" => %{operations: [:choice_source]}
        })
        |> Map.put(:source_relationships, %{
          123 => %{target_domain: :customers, source_field: :customer_id, target_field: :id},
          bad_config: [:not, :a, :map],
          missing_keys: %{target_domain: :customers},
          bad_target_domain: %{target_domain: 456, source_field: :customer_id, target_field: :id},
          missing_source_field: %{
            target_domain: :customers,
            source_field: :missing_customer_id,
            target_field: :id
          },
          bad_source_field: %{target_domain: :customers, source_field: 789, target_field: :id},
          bad_target_field: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: 987
          },
          bad_relationship_source_path: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            source_path: ".customers"
          },
          bad_relationship_filters: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            filters: %{active: true}
          },
          bad_relationship_filter_operator: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            filters: [{:unknown, "customers.active", true}]
          },
          bad_relationship_filter_path: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            filters: [{:eq, ".active", true}]
          },
          bad_relationship_filter_expression: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            filters: [123]
          },
          bad_relationship_filter_operands: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            filters: [{:and, {:eq, "customers.active", true}}]
          },
          bad_virtual_join: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            virtual_join: :customer
          },
          bad_virtual_join_entry: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            virtual_join: [123]
          },
          missing_virtual_join_keys: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            virtual_join: [%{working_field: :customer_id}]
          },
          bad_virtual_join_working_field: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            virtual_join: [%{working_field: 789, source_field: "customers.id"}]
          },
          missing_virtual_join_working_field: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            virtual_join: [%{working_field: :missing_customer_id, source_field: "customers.id"}]
          },
          bad_virtual_join_source_field: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            virtual_join: [%{working_field: :customer_id, source_field: ".id"}]
          },
          bad_virtual_join_required: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            virtual_join: [
              %{working_field: :customer_id, source_field: "customers.id", required: :yes}
            ]
          }
        })
        |> Map.put(:choice_sources, %{
          456 => %{domain: :customers, value_field: :id, label_field: :name},
          bad_config: [:not, :a, :map],
          missing_keys: %{domain: :customers},
          bad_domain: %{domain: 111, value_field: :id, label_field: :name},
          bad_value_field: %{domain: :customers, value_field: 222, label_field: :name},
          bad_label_field: %{domain: :customers, value_field: :id, label_field: 333},
          missing_relationship: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            source_relationship: :missing_customer
          },
          bad_relationship: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            source_relationship: 444
          },
          missing_capability: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            capability: "customer.missing"
          },
          bad_capability: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            capability: 555
          },
          bad_source_path: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            source_path: ""
          },
          bad_value_source: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            value_source: 666
          },
          bad_caption_source: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            caption_source: ".name"
          },
          bad_description_source: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            description_source: ["customers.description"]
          },
          bad_filters: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            filters: %{active: true}
          },
          bad_filter_operator: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            filters: [{:unknown, "customers.active", true}]
          },
          bad_filter_path: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            filters: [{:eq, ".active", true}]
          },
          bad_filter_expression: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            filters: [123]
          },
          bad_filter_operands: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            filters: [{:and, {:eq, "customers.active", true}}]
          },
          bad_order_by: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            order_by: :name
          },
          bad_order_by_entry: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            order_by: [123]
          },
          bad_order_by_direction: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            order_by: [{"customers.name", :sideways}]
          },
          bad_presentation: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            presentation: :select
          },
          bad_presentation_control: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            presentation: %{control: :wizard}
          },
          bad_presentation_mode: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            presentation: %{mode: :sparkly}
          },
          bad_presentation_cardinality: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            presentation: %{cardinality: :some}
          }
        })
        |> put_in([:source, :columns, :status, :choice_source], :missing_status_choices)
        |> put_in([:source, :columns, :id, :reference], %{choice_source: 777})
        |> put_in([:source, :columns, :customer_id, :reference], %{
          choice_source: :missing_ref_choices,
          value_source: 888,
          caption_source: 999,
          caption_field: :missing_customer_name
        })
        |> Map.put(:columns, %{
          status: %{choice_source: 555},
          customer_id: %{reference: :customer_choices}
        })

      assert {:error, diagnostics} = Domain.validate(domain)

      assert %{
               code: :invalid_source_relationship_id,
               source_relationship: 123,
               path: [:source_relationships, 123]
             } = error_for(diagnostics, :invalid_source_relationship_id)

      assert Enum.any?(
               errors_for(diagnostics, :invalid_section_shape),
               &match?(
                 %{source_relationship: :bad_config, path: [:source_relationships, :bad_config]},
                 &1
               )
             )

      assert %{
               code: :source_relationship_missing_required_keys,
               source_relationship: :missing_keys,
               path: [:source_relationships, :missing_keys]
             } = error_for(diagnostics, :source_relationship_missing_required_keys)

      assert %{
               code: :invalid_source_relationship_target_domain,
               source_relationship: :bad_target_domain,
               target_domain: 456,
               path: [:source_relationships, :bad_target_domain, :target_domain]
             } = error_for(diagnostics, :invalid_source_relationship_target_domain)

      assert %{
               code: :source_relationship_source_field_not_found,
               source_relationship: :missing_source_field,
               source_field: :missing_customer_id,
               path: [:source_relationships, :missing_source_field, :source_field]
             } = error_for(diagnostics, :source_relationship_source_field_not_found)

      assert %{
               code: :invalid_source_relationship_source_field,
               source_relationship: :bad_source_field,
               source_field: 789,
               path: [:source_relationships, :bad_source_field, :source_field]
             } = error_for(diagnostics, :invalid_source_relationship_source_field)

      assert %{
               code: :invalid_source_relationship_target_field,
               source_relationship: :bad_target_field,
               target_field: 987,
               path: [:source_relationships, :bad_target_field, :target_field]
             } = error_for(diagnostics, :invalid_source_relationship_target_field)

      assert %{
               code: :invalid_source_relationship_source_path,
               source_relationship: :bad_relationship_source_path,
               source_path: ".customers",
               path: [:source_relationships, :bad_relationship_source_path, :source_path]
             } = error_for(diagnostics, :invalid_source_relationship_source_path)

      assert %{
               code: :invalid_source_relationship_filters,
               source_relationship: :bad_relationship_filters,
               path: [:source_relationships, :bad_relationship_filters, :filters]
             } = error_for(diagnostics, :invalid_source_relationship_filters)

      assert %{
               code: :invalid_source_relationship_filter_operator,
               source_relationship: :bad_relationship_filter_operator,
               operator: :unknown,
               path: [:source_relationships, :bad_relationship_filter_operator, :filters, 0]
             } = error_for(diagnostics, :invalid_source_relationship_filter_operator)

      assert %{
               code: :invalid_source_relationship_filter_path,
               source_relationship: :bad_relationship_filter_path,
               field: ".active",
               path: [:source_relationships, :bad_relationship_filter_path, :filters, 0, :field]
             } = error_for(diagnostics, :invalid_source_relationship_filter_path)

      assert %{
               code: :invalid_source_relationship_filter_expression,
               source_relationship: :bad_relationship_filter_expression,
               filter: 123,
               path: [:source_relationships, :bad_relationship_filter_expression, :filters, 0]
             } = error_for(diagnostics, :invalid_source_relationship_filter_expression)

      assert %{
               code: :invalid_source_relationship_filter_operands,
               source_relationship: :bad_relationship_filter_operands,
               operator: :and,
               path: [:source_relationships, :bad_relationship_filter_operands, :filters, 0]
             } = error_for(diagnostics, :invalid_source_relationship_filter_operands)

      assert %{
               code: :invalid_source_relationship_virtual_join,
               source_relationship: :bad_virtual_join,
               path: [:source_relationships, :bad_virtual_join, :virtual_join]
             } = error_for(diagnostics, :invalid_source_relationship_virtual_join)

      assert %{
               code: :invalid_source_relationship_virtual_join_entry,
               source_relationship: :bad_virtual_join_entry,
               path: [:source_relationships, :bad_virtual_join_entry, :virtual_join, 0]
             } = error_for(diagnostics, :invalid_source_relationship_virtual_join_entry)

      assert %{
               code: :source_relationship_virtual_join_missing_required_keys,
               source_relationship: :missing_virtual_join_keys,
               keys: [:source_field],
               path: [:source_relationships, :missing_virtual_join_keys, :virtual_join, 0]
             } = error_for(diagnostics, :source_relationship_virtual_join_missing_required_keys)

      assert %{
               code: :invalid_source_relationship_virtual_join_working_field,
               source_relationship: :bad_virtual_join_working_field,
               working_field: 789,
               path: [
                 :source_relationships,
                 :bad_virtual_join_working_field,
                 :virtual_join,
                 0,
                 :working_field
               ]
             } = error_for(diagnostics, :invalid_source_relationship_virtual_join_working_field)

      assert %{
               code: :source_relationship_virtual_join_working_field_not_found,
               source_relationship: :missing_virtual_join_working_field,
               working_field: :missing_customer_id,
               path: [
                 :source_relationships,
                 :missing_virtual_join_working_field,
                 :virtual_join,
                 0,
                 :working_field
               ]
             } =
               error_for(diagnostics, :source_relationship_virtual_join_working_field_not_found)

      assert %{
               code: :invalid_source_relationship_virtual_join_source_field,
               source_relationship: :bad_virtual_join_source_field,
               source_field: ".id",
               path: [
                 :source_relationships,
                 :bad_virtual_join_source_field,
                 :virtual_join,
                 0,
                 :source_field
               ]
             } = error_for(diagnostics, :invalid_source_relationship_virtual_join_source_field)

      assert %{
               code: :invalid_source_relationship_virtual_join_required,
               source_relationship: :bad_virtual_join_required,
               required: :yes,
               path: [
                 :source_relationships,
                 :bad_virtual_join_required,
                 :virtual_join,
                 0,
                 :required
               ]
             } = error_for(diagnostics, :invalid_source_relationship_virtual_join_required)

      assert %{
               code: :invalid_choice_source_id,
               choice_source: 456,
               path: [:choice_sources, 456]
             } = error_for(diagnostics, :invalid_choice_source_id)

      assert Enum.any?(
               errors_for(diagnostics, :invalid_section_shape),
               &match?(%{choice_source: :bad_config, path: [:choice_sources, :bad_config]}, &1)
             )

      assert %{
               code: :choice_source_missing_required_keys,
               choice_source: :missing_keys,
               path: [:choice_sources, :missing_keys]
             } = error_for(diagnostics, :choice_source_missing_required_keys)

      assert %{
               code: :invalid_choice_source_domain,
               choice_source: :bad_domain,
               domain: 111,
               path: [:choice_sources, :bad_domain, :domain]
             } = error_for(diagnostics, :invalid_choice_source_domain)

      assert %{
               code: :invalid_choice_source_value_field,
               choice_source: :bad_value_field,
               value_field: 222,
               path: [:choice_sources, :bad_value_field, :value_field]
             } = error_for(diagnostics, :invalid_choice_source_value_field)

      assert %{
               code: :invalid_choice_source_label_field,
               choice_source: :bad_label_field,
               label_field: 333,
               path: [:choice_sources, :bad_label_field, :label_field]
             } = error_for(diagnostics, :invalid_choice_source_label_field)

      assert %{
               code: :choice_source_relationship_not_found,
               choice_source: :missing_relationship,
               source_relationship: :missing_customer,
               path: [:choice_sources, :missing_relationship, :source_relationship]
             } = error_for(diagnostics, :choice_source_relationship_not_found)

      assert %{
               code: :invalid_choice_source_relationship,
               choice_source: :bad_relationship,
               source_relationship: 444,
               path: [:choice_sources, :bad_relationship, :source_relationship]
             } = error_for(diagnostics, :invalid_choice_source_relationship)

      assert %{
               code: :choice_source_capability_not_found,
               choice_source: :missing_capability,
               capability: "customer.missing",
               path: [:choice_sources, :missing_capability, :capability]
             } = error_for(diagnostics, :choice_source_capability_not_found)

      assert %{
               code: :invalid_choice_source_capability,
               choice_source: :bad_capability,
               capability: 555,
               path: [:choice_sources, :bad_capability, :capability]
             } = error_for(diagnostics, :invalid_choice_source_capability)

      assert %{
               code: :invalid_choice_source_source_path,
               choice_source: :bad_source_path,
               attribute: :source_path,
               value: "",
               path: [:choice_sources, :bad_source_path, :source_path]
             } = error_for(diagnostics, :invalid_choice_source_source_path)

      assert %{
               code: :invalid_choice_source_value_source,
               choice_source: :bad_value_source,
               attribute: :value_source,
               value: 666,
               path: [:choice_sources, :bad_value_source, :value_source]
             } = error_for(diagnostics, :invalid_choice_source_value_source)

      assert %{
               code: :invalid_choice_source_caption_source,
               choice_source: :bad_caption_source,
               attribute: :caption_source,
               value: ".name",
               path: [:choice_sources, :bad_caption_source, :caption_source]
             } = error_for(diagnostics, :invalid_choice_source_caption_source)

      assert %{
               code: :invalid_choice_source_description_source,
               choice_source: :bad_description_source,
               attribute: :description_source,
               value: ["customers.description"],
               path: [:choice_sources, :bad_description_source, :description_source]
             } = error_for(diagnostics, :invalid_choice_source_description_source)

      assert %{
               code: :invalid_choice_source_filters,
               choice_source: :bad_filters,
               path: [:choice_sources, :bad_filters, :filters]
             } = error_for(diagnostics, :invalid_choice_source_filters)

      assert %{
               code: :invalid_choice_source_filter_operator,
               choice_source: :bad_filter_operator,
               operator: :unknown,
               path: [:choice_sources, :bad_filter_operator, :filters, 0]
             } = error_for(diagnostics, :invalid_choice_source_filter_operator)

      assert %{
               code: :invalid_choice_source_filter_path,
               choice_source: :bad_filter_path,
               field: ".active",
               path: [:choice_sources, :bad_filter_path, :filters, 0, :field]
             } = error_for(diagnostics, :invalid_choice_source_filter_path)

      assert %{
               code: :invalid_choice_source_filter_expression,
               choice_source: :bad_filter_expression,
               filter: 123,
               path: [:choice_sources, :bad_filter_expression, :filters, 0]
             } = error_for(diagnostics, :invalid_choice_source_filter_expression)

      assert %{
               code: :invalid_choice_source_filter_operands,
               choice_source: :bad_filter_operands,
               operator: :and,
               path: [:choice_sources, :bad_filter_operands, :filters, 0]
             } = error_for(diagnostics, :invalid_choice_source_filter_operands)

      assert %{
               code: :invalid_choice_source_order_by,
               choice_source: :bad_order_by,
               path: [:choice_sources, :bad_order_by, :order_by]
             } = error_for(diagnostics, :invalid_choice_source_order_by)

      assert %{
               code: :invalid_choice_source_order_by_entry,
               choice_source: :bad_order_by_entry,
               path: [:choice_sources, :bad_order_by_entry, :order_by, 0]
             } = error_for(diagnostics, :invalid_choice_source_order_by_entry)

      assert %{
               code: :invalid_choice_source_order_by_direction,
               choice_source: :bad_order_by_direction,
               direction: :sideways,
               path: [:choice_sources, :bad_order_by_direction, :order_by, 0]
             } = error_for(diagnostics, :invalid_choice_source_order_by_direction)

      assert %{
               code: :invalid_choice_source_presentation,
               choice_source: :bad_presentation,
               path: [:choice_sources, :bad_presentation, :presentation]
             } = error_for(diagnostics, :invalid_choice_source_presentation)

      assert %{
               code: :invalid_choice_source_presentation_control,
               choice_source: :bad_presentation_control,
               attribute: :control,
               value: :wizard,
               path: [:choice_sources, :bad_presentation_control, :presentation, :control]
             } = error_for(diagnostics, :invalid_choice_source_presentation_control)

      assert %{
               code: :invalid_choice_source_presentation_mode,
               choice_source: :bad_presentation_mode,
               attribute: :mode,
               value: :sparkly,
               path: [:choice_sources, :bad_presentation_mode, :presentation, :mode]
             } = error_for(diagnostics, :invalid_choice_source_presentation_mode)

      assert %{
               code: :invalid_choice_source_presentation_cardinality,
               choice_source: :bad_presentation_cardinality,
               attribute: :cardinality,
               value: :some,
               path: [
                 :choice_sources,
                 :bad_presentation_cardinality,
                 :presentation,
                 :cardinality
               ]
             } = error_for(diagnostics, :invalid_choice_source_presentation_cardinality)

      assert %{
               code: :field_choice_source_not_found,
               field: :status,
               choice_source: :missing_status_choices,
               path: [:source, :columns, :status, :choice_source]
             } = error_for(diagnostics, :field_choice_source_not_found)

      assert %{
               code: :invalid_field_choice_source,
               field: :status,
               choice_source: 555,
               path: [:columns, :status, :choice_source]
             } = error_for(diagnostics, :invalid_field_choice_source)

      assert %{
               code: :invalid_field_reference,
               field: :customer_id,
               path: [:columns, :customer_id, :reference]
             } = error_for(diagnostics, :invalid_field_reference)

      assert %{
               code: :invalid_field_reference_choice_source,
               field: :id,
               choice_source: 777,
               path: [:source, :columns, :id, :reference, :choice_source]
             } = error_for(diagnostics, :invalid_field_reference_choice_source)

      assert %{
               code: :field_reference_choice_source_not_found,
               field: :customer_id,
               choice_source: :missing_ref_choices,
               path: [:source, :columns, :customer_id, :reference, :choice_source]
             } = error_for(diagnostics, :field_reference_choice_source_not_found)

      assert %{
               code: :invalid_field_reference_value_source,
               field: :customer_id,
               value_source: 888,
               path: [:source, :columns, :customer_id, :reference, :value_source]
             } = error_for(diagnostics, :invalid_field_reference_value_source)

      assert %{
               code: :invalid_field_reference_caption_source,
               field: :customer_id,
               caption_source: 999,
               path: [:source, :columns, :customer_id, :reference, :caption_source]
             } = error_for(diagnostics, :invalid_field_reference_caption_source)

      assert %{
               code: :field_reference_caption_field_not_found,
               field: :customer_id,
               caption_field: :missing_customer_name,
               path: [:source, :columns, :customer_id, :reference, :caption_field]
             } = error_for(diagnostics, :field_reference_caption_field_not_found)
    end

    test "DomainValidator normalized mode returns contract errors before legacy tuples" do
      domain =
        valid_domain()
        |> put_in([:source, :fields], [:id, :status])
        |> put_in([:source, :columns], %{id: %{type: :integer}})

      assert {:error, [%{code: :source_field_missing_column, field: :status} | _]} =
               DomainValidator.validate_domain(domain, normalize: true)
    end
  end

  defp error_for(diagnostics, code) do
    Enum.find(diagnostics.errors, &(&1.code == code))
  end

  defp errors_for(diagnostics, code) do
    Enum.filter(diagnostics.errors, &(&1.code == code))
  end

  defp valid_domain do
    %{
      source: valid_source(),
      schemas: %{
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
      },
      joins: %{
        customer: %{type: :left}
      },
      filters: %{
        "status_picker" => %{field: :status, type: :string}
      },
      required_filters: [{"status", "open"}]
    }
  end

  defp valid_source do
    %{
      source_table: "orders",
      primary_key: :id,
      fields: [:id, :status, :customer_id],
      columns: %{
        id: %{type: :integer},
        status: %{type: :string},
        customer_id: %{type: :integer}
      },
      associations: %{
        customer: %{
          queryable: :customers,
          field: :customer,
          owner_key: :customer_id,
          related_key: :id
        }
      }
    }
  end
end
