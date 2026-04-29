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
