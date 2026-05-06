defmodule Selecto.DomainChoicesTest do
  use ExUnit.Case, async: true

  alias Selecto.Domain
  alias Selecto.Domain.Choices
  alias Selecto.Domain.Choices.{OptionsRequest, OptionsResult, Request, Result}

  describe "request/4" do
    test "builds a membership request from a source field reference binding" do
      assert {:ok, %Request{} = request} =
               Choices.request(valid_domain(), :customer_id, 42,
                 actor: :current_user,
                 tenant: "tenant-1",
                 record: %{customer_id: 42},
                 filters: [{"status", "open"}],
                 context: %{surface: :test}
               )

      assert request.domain == :orders
      assert request.field == :customer_id
      assert request.value == 42
      assert request.choice_source == :customer_choices
      assert request.choice_source_config.domain == :customers
      assert request.source_relationship == :customer
      assert request.source_relationship_config.source_field == :customer_id
      assert request.reference.choice_source == :customer_choices
      assert request.reference.caption_field == :customer_name
      assert request.actor == :current_user
      assert request.tenant == "tenant-1"
      assert request.record == %{customer_id: 42}
      assert request.filters == [{"status", "open"}]

      assert request.constraint_filters == %{
               source_relationship: [{:eq, "customers.active", true}],
               choice_source: [["eq", "customers.available", true]],
               domain_of_interest: [{"status", "open"}]
             }

      assert request.context == %{surface: :test}
    end

    test "accepts an already normalized domain" do
      {:ok, normalized, _diagnostics} = Domain.validate(valid_domain())

      assert {:ok, %Request{choice_source: :customer_choices}} =
               Choices.request(normalized, :customer_id, 42)
    end

    test "resolves compact projection bindings" do
      domain =
        valid_domain()
        |> update_in([:source, :columns, :customer_id], &Map.delete(&1, :reference))
        |> Map.put(:columns, %{
          customer_id: %{choice_source: "customer_choices"}
        })

      assert {:ok, binding} = Choices.binding(domain, "customer_id")

      assert binding.binding_kind == :choice_source
      assert binding.choice_source == "customer_choices"
      assert binding.field == "customer_id"
    end

    test "resolves schema field reference bindings" do
      domain =
        valid_domain()
        |> put_in([:schemas, :customers, :columns, :id, :reference], %{
          choice_source: :customer_choices
        })

      assert {:ok, %Request{} = request} = Choices.request(domain, "customers.id", 42)

      assert request.field == "customers.id"
      assert request.choice_source == :customer_choices
      assert request.field_binding.path == [:schemas, "customers", :columns, "id"]
    end

    test "returns structured errors for unbound fields and invalid domains" do
      assert {:error, %{code: :field_choice_source_not_bound, field: :status}} =
               Choices.request(valid_domain(), :status, "ready")

      assert {:error, %{code: :invalid_domain_contract, errors: errors}} =
               Choices.request(%{source: valid_source()}, :customer_id, 42)

      assert Enum.any?(errors, &match?(%{code: :missing_required_section}, &1))
    end
  end

  describe "validate_choice/4" do
    test "returns unknown without a resolver" do
      assert {:error, %Result{} = result} =
               Choices.validate_choice(valid_domain(), :customer_id, 42)

      assert result.status == :unknown
      assert result.reason_code == :resolver_required
      assert %Request{field: :customer_id, value: 42} = result.request
    end

    test "uses an explicit resolver and normalizes the result request" do
      resolver = fn %Request{value: 42, choice_source: :customer_choices} ->
        Choices.valid(:fixture_member)
      end

      assert {:ok, %Result{} = result} =
               Choices.validate_choice(valid_domain(), :customer_id, 42, resolver: resolver)

      assert result.status == :valid
      assert result.reason_code == :fixture_member
      assert %Request{field: :customer_id, value: 42} = result.request
      assert Choices.valid_choice?(valid_domain(), :customer_id, 42, resolver: resolver)
    end
  end

  describe "options_request/3" do
    test "builds an option-list request from a field binding" do
      assert {:ok, %OptionsRequest{} = request} =
               Choices.options_request(valid_domain(), :customer_id,
                 actor: :current_user,
                 tenant: "tenant-1",
                 record: %{status: "ready"},
                 context: %{surface: :components},
                 search: "acme",
                 limit: 20,
                 offset: 10,
                 filters: [{"status", "ready"}]
               )

      assert request.domain == :orders
      assert request.field == :customer_id
      assert request.choice_source == :customer_choices
      assert request.choice_source_config.domain == :customers
      assert request.source_relationship == :customer
      assert request.source_relationship_config.source_field == :customer_id
      assert request.reference.choice_source == :customer_choices
      assert request.actor == :current_user
      assert request.tenant == "tenant-1"
      assert request.record == %{status: "ready"}
      assert request.context == %{surface: :components}
      assert request.filters == [{"status", "ready"}]

      assert request.constraint_filters == %{
               source_relationship: [{:eq, "customers.active", true}],
               choice_source: [["eq", "customers.available", true]],
               domain_of_interest: [{"status", "ready"}]
             }

      assert request.search == "acme"
      assert request.limit == 20
      assert request.offset == 10
    end

    test "builds an option-list request directly from a choice source" do
      assert {:ok, %OptionsRequest{} = request} =
               Choices.options_request(valid_domain(), :customer_choices,
                 by: :choice_source,
                 search: "north"
               )

      assert request.field == nil
      assert request.choice_source == :customer_choices
      assert request.choice_source_config.label_field == :name
      assert request.source_relationship == :customer
      assert request.search == "north"
    end

    test "returns structured errors for unbound option-list fields" do
      assert {:error, %{code: :field_choice_source_not_bound, field: :status}} =
               Choices.options_request(valid_domain(), :status)
    end
  end

  describe "list_options/3" do
    test "returns unknown without a resolver" do
      assert {:error, %OptionsResult{} = result} =
               Choices.list_options(valid_domain(), :customer_id, search: "acme")

      assert result.status == :unknown
      assert result.reason_code == :resolver_required
      assert %OptionsRequest{field: :customer_id, search: "acme"} = result.request
    end

    test "uses an explicit resolver and normalizes the result request" do
      resolver = fn %OptionsRequest{choice_source: :customer_choices, search: "acme"} ->
        Choices.options_resolved(
          [
            %{value: 42, label: "Acme Camps"}
          ],
          total_count: 1
        )
      end

      assert {:ok, %OptionsResult{} = result} =
               Choices.list_options(valid_domain(), :customer_id,
                 search: "acme",
                 resolver: resolver
               )

      assert result.status == :resolved
      assert result.reason_code == :options_resolved
      assert result.options == [%{value: 42, label: "Acme Camps"}]
      assert result.total_count == 1
      assert %OptionsRequest{field: :customer_id, search: "acme"} = result.request
    end
  end

  defp valid_domain do
    %{
      schema_version: 1,
      name: :orders,
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
      capabilities: %{
        "customer.choose" => %{operations: [:choice_source]}
      },
      source_relationships: %{
        customer: %{
          target_domain: :customers,
          source_field: :customer_id,
          target_field: :id,
          filters: [{:eq, "customers.active", true}]
        }
      },
      choice_sources: %{
        customer_choices: %{
          domain: :customers,
          value_field: :id,
          label_field: :name,
          source_relationship: :customer,
          capability: "customer.choose",
          filters: [["eq", "customers.available", true]]
        }
      }
    }
  end

  defp valid_source do
    %{
      source_table: "orders",
      primary_key: :id,
      fields: [:id, :status, :customer_id, :customer_name],
      columns: %{
        id: %{type: :integer},
        status: %{type: :string},
        customer_id: %{
          type: :integer,
          reference: %{
            choice_source: :customer_choices,
            value_source: "customers.id",
            caption_source: "customers.name",
            caption_field: :customer_name
          }
        },
        customer_name: %{type: :string}
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
