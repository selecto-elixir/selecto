defmodule Selecto.Config.OverlayDSLTest do
  use ExUnit.Case, async: true

  def __query_member_cte__(_selecto), do: :ok
  def __query_member_subquery__(_selecto), do: :ok

  describe "OverlayDSL basic usage" do
    test "generates overlay map from DSL" do
      defmodule TestOverlay1 do
        use Selecto.Config.OverlayDSL

        defcolumn :price do
          label("Product Price")
          format(:currency)
        end

        deffilter "status" do
          name("Status Filter")
          type(:string)
        end
      end

      overlay = TestOverlay1.overlay()

      assert overlay.columns[:price].label == "Product Price"
      assert overlay.columns[:price].format == :currency
      assert overlay.filters["status"].name == "Status Filter"
      assert overlay.filters["status"].type == :string
    end

    test "supports @redactions module attribute" do
      defmodule TestOverlay2 do
        use Selecto.Config.OverlayDSL

        @redactions [:password, :secret_key]
      end

      overlay = TestOverlay2.overlay()

      assert overlay.redact_fields == [:password, :secret_key]
    end

    test "handles multiple column definitions" do
      defmodule TestOverlay3 do
        use Selecto.Config.OverlayDSL

        defcolumn :price do
          label("Price")
          format(:currency)
          precision(2)
        end

        defcolumn :quantity do
          label("Quantity")
          aggregate_functions([:sum, :count])
        end

        defcolumn :active do
          label("Active Status")
          format(:yes_no)
        end
      end

      overlay = TestOverlay3.overlay()

      assert map_size(overlay.columns) == 3
      assert overlay.columns[:price].format == :currency
      assert overlay.columns[:quantity].aggregate_functions == [:sum, :count]
      assert overlay.columns[:active].format == :yes_no
    end

    test "handles multiple filter definitions" do
      defmodule TestOverlay4 do
        use Selecto.Config.OverlayDSL

        deffilter "price_range" do
          name("Price Range")
          type(:string)
          description("Filter by price range")
        end

        deffilter "in_stock" do
          name("In Stock")
          type(:boolean)
          default(true)
        end
      end

      overlay = TestOverlay4.overlay()

      assert map_size(overlay.filters) == 2
      assert overlay.filters["price_range"].description == "Filter by price range"
      assert overlay.filters["in_stock"].default == true
    end

    test "builds source relationship and choice source registries" do
      defmodule TestChoiceSourceOverlay do
        use Selecto.Config.OverlayDSL

        defcolumn :assignee_id do
          label("Assignee")
          choice_source(:work_item_assignees)

          reference(%{
            choice_source: :work_item_assignees,
            value_source: "assignee.id",
            caption_source: "assignee.full_name",
            caption_field: "assignee.full_name"
          })
        end

        defsource_relationship(:work_item_assignee, %{
          target_domain: :employee,
          source_field: :assignee_id,
          target_field: :id,
          source_path: "assignee"
        })

        defchoice_source(:work_item_assignees, %{
          domain: :employee,
          value_field: :id,
          label_field: :full_name,
          source_relationship: :work_item_assignee,
          presentation: %{control: :autocomplete, mode: :async, cardinality: :one}
        })
      end

      overlay = TestChoiceSourceOverlay.overlay()

      assert overlay.columns.assignee_id.choice_source == :work_item_assignees
      assert overlay.columns.assignee_id.reference.choice_source == :work_item_assignees
      assert overlay.source_relationships.work_item_assignee.source_field == :assignee_id
      assert overlay.choice_sources.work_item_assignees.domain == :employee
      assert overlay.choice_sources.work_item_assignees.label_field == :full_name
      assert overlay.choice_sources.work_item_assignees.presentation.control == :autocomplete
    end

    test "builds write contract, actions, and capabilities" do
      defmodule TestWriteContractOverlay do
        use Selecto.Config.OverlayDSL

        defwrite_operation :insert do
          enabled(true)
          returning(:record)
        end

        defwrite_operation(:delete, %{enabled: true, require_filter: true})

        defwrite_field :title do
          insertable(true)
          updatable(true)
          required_on([:insert])
        end

        defwrite_field(:inserted_at, %{
          insertable: false,
          updatable: false,
          server_managed: true
        })

        defwrite_relationship :comments do
          writable(true)
          cardinality(:many)
          allowed_ops([:insert, :update])
          foreign_key(:work_item_id)
        end

        defwrite_transition(:state, %{draft: [:open], open: [:closed]})
        defwrite_validation({:required_if, :title, :state, "open"})
        defwrite_constraint({:require_relationship, :comments})
        defwrite_tenant_scope(%{required: true, field: :tenant_id})
        defwrite_hook(:before_validate, [{Selecto.Config.OverlayDSLTest, :example_hook}])

        defaction :approve do
          type(:transition)
          capability("work_item.approve")
          transition(%{field: :state, from: :open, to: :approved})
          execution(%{kind: :updato, operation: :update, set: %{state: :approved}})
        end

        defcapability "work_item.approve" do
          operations([:action, :update])
          action(:approve)
        end
      end

      overlay = TestWriteContractOverlay.overlay()

      assert overlay.writes.operations.insert == %{enabled: true, returning: :record}
      assert overlay.writes.operations.delete == %{enabled: true, require_filter: true}

      assert overlay.writes.fields.title == %{
               insertable: true,
               updatable: true,
               required_on: [:insert]
             }

      assert overlay.writes.fields.inserted_at == %{
               insertable: false,
               updatable: false,
               server_managed: true
             }

      assert overlay.writes.relationships.comments == %{
               writable: true,
               cardinality: :many,
               allowed_ops: [:insert, :update],
               foreign_key: :work_item_id
             }

      assert overlay.writes.transitions.state == %{draft: [:open], open: [:closed]}
      assert overlay.writes.validations == [{:required_if, :title, :state, "open"}]
      assert overlay.writes.constraints == [{:require_relationship, :comments}]
      assert overlay.writes.scope.tenant == %{required: true, field: :tenant_id}

      assert overlay.writes.hooks.before_validate == [
               {Selecto.Config.OverlayDSLTest, :example_hook}
             ]

      assert overlay.actions.approve.transition == %{
               field: :state,
               from: :open,
               to: :approved
             }

      assert overlay.actions.approve.execution == %{
               kind: :updato,
               operation: :update,
               set: %{state: :approved}
             }

      assert overlay.capabilities["work_item.approve"] == %{
               operations: [:action, :update],
               action: :approve
             }
    end
  end

  describe "column directives" do
    test "label directive" do
      defmodule TestColumnLabel do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          label("Custom Label")
        end
      end

      assert TestColumnLabel.overlay().columns[:field1].label == "Custom Label"
    end

    test "format directive" do
      defmodule TestColumnFormat do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          format(:currency)
        end
      end

      assert TestColumnFormat.overlay().columns[:field1].format == :currency
    end

    test "aggregate_functions directive" do
      defmodule TestColumnAggregates do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          aggregate_functions([:sum, :avg, :max])
        end
      end

      assert TestColumnAggregates.overlay().columns[:field1].aggregate_functions == [
               :sum,
               :avg,
               :max
             ]
    end

    test "precision directive" do
      defmodule TestColumnPrecision do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          precision(2)
        end
      end

      assert TestColumnPrecision.overlay().columns[:field1].precision == 2
    end

    test "max_length directive" do
      defmodule TestColumnMaxLength do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          max_length(100)
        end
      end

      assert TestColumnMaxLength.overlay().columns[:field1].max_length == 100
    end

    test "sortable directive" do
      defmodule TestColumnSortable do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          sortable(false)
        end
      end

      assert TestColumnSortable.overlay().columns[:field1].sortable == false
    end

    test "filterable directive" do
      defmodule TestColumnFilterable do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          filterable(true)
        end
      end

      assert TestColumnFilterable.overlay().columns[:field1].filterable == true
    end

    test "computed directive" do
      defmodule TestColumnComputed do
        use Selecto.Config.OverlayDSL

        defcolumn :field1 do
          computed(true)
        end
      end

      assert TestColumnComputed.overlay().columns[:field1].computed == true
    end

    test "multiple directives in one column" do
      defmodule TestColumnMultiple do
        use Selecto.Config.OverlayDSL

        defcolumn :price do
          label("Product Price")
          format(:currency)
          precision(2)
          aggregate_functions([:sum, :avg])
          sortable(true)
          filterable(true)
        end
      end

      column = TestColumnMultiple.overlay().columns[:price]

      assert column.label == "Product Price"
      assert column.format == :currency
      assert column.precision == 2
      assert column.aggregate_functions == [:sum, :avg]
      assert column.sortable == true
      assert column.filterable == true
    end
  end

  describe "query member macros" do
    test "builds named cte, values, and subquery presets" do
      defmodule TestQueryMembersOverlay do
        use Selecto.Config.OverlayDSL

        defcte :employee_pets do
          query(&Selecto.Config.OverlayDSLTest.__query_member_cte__/1)
          columns(["employee_id", "pet_name"])
          join(owner_key: :id, related_key: :employee_id, fields: :infer)
        end

        defvalues :status_lookup do
          rows([["active", "Active"], ["inactive", "Inactive"]])
          columns(["status", "label"])
          as("status_lookup")
          join(owner_key: :status, related_key: :status)
        end

        defsubquery :high_value_orders do
          query(&Selecto.Config.OverlayDSLTest.__query_member_subquery__/1)
          type(:inner)
          on([%{left: "id", right: "customer_id"}])
        end

        deflateral :tag_expansion do
          source({:unnest, "\"selecto_root\".\"tags\""})
          as("tag_expansion")
          join_type(:inner)
        end

        defunnest :product_tags do
          array_field("tags")
          as("product_tag")
          ordinality("product_tag_position")
        end
      end

      overlay = TestQueryMembersOverlay.overlay()

      assert overlay.query_members.ctes.employee_pets.columns == ["employee_id", "pet_name"]
      assert is_function(overlay.query_members.ctes.employee_pets.query, 1)

      assert overlay.query_members.ctes.employee_pets.join == [
               owner_key: :id,
               related_key: :employee_id,
               fields: :infer
             ]

      assert overlay.query_members.values.status_lookup.rows == [
               ["active", "Active"],
               ["inactive", "Inactive"]
             ]

      assert overlay.query_members.values.status_lookup.as == "status_lookup"
      assert overlay.query_members.subqueries.high_value_orders.type == :inner
      assert is_function(overlay.query_members.subqueries.high_value_orders.query, 1)

      assert overlay.query_members.subqueries.high_value_orders.on == [
               %{left: "id", right: "customer_id"}
             ]

      assert overlay.query_members.laterals.tag_expansion.source ==
               {:unnest, "\"selecto_root\".\"tags\""}

      assert overlay.query_members.laterals.tag_expansion.join_type == :inner
      assert overlay.query_members.unnests.product_tags.array_field == "tags"
      assert overlay.query_members.unnests.product_tags.ordinality == "product_tag_position"
    end
  end

  describe "function macros" do
    test "builds named UDF specs from DSL" do
      defmodule TestFunctionOverlay do
        use Selecto.Config.OverlayDSL

        deffunction "similarity" do
          kind(:scalar)
          sql_name("public.similarity")
          returns(:float)
          allowed_in([:select, :order_by])
          arg(:left, :string, source: :selector)
          arg(:right, :string, source: :value)
        end
      end

      overlay = TestFunctionOverlay.overlay()

      assert overlay.functions["similarity"].kind == :scalar
      assert overlay.functions["similarity"].sql_name == "public.similarity"
      assert overlay.functions["similarity"].returns == :float
      assert overlay.functions["similarity"].allowed_in == [:select, :order_by]

      assert overlay.functions["similarity"].args == [
               %{name: :left, type: :string, source: :selector},
               %{name: :right, type: :string, source: :value}
             ]
    end
  end

  describe "detail action macros" do
    test "builds detail actions from DSL" do
      defmodule TestDetailActionsOverlay do
        use Selecto.Config.OverlayDSL

        defdetail_action :customer_profile do
          name("Customer Profile")
          description("Open the customer profile in a new tab")
          type(:external_link)
          required_fields([:customer_id])
          payload(%{url_template: "https://example.test/customers/{{customer_id}}"})
        end

        defpopup :customer_modal do
          name("Customer Modal")
          description("Show customer details in a modal")
          required_fields([:customer_id, :full_name])
          payload(%{title: "Customer {{full_name}}", size: :xl})
        end
      end

      overlay = TestDetailActionsOverlay.overlay()

      assert overlay.detail_actions.customer_profile.type == :external_link
      assert overlay.detail_actions.customer_profile.required_fields == [:customer_id]

      assert overlay.detail_actions.customer_profile.payload.url_template ==
               "https://example.test/customers/{{customer_id}}"

      assert overlay.detail_actions.customer_modal.type == :modal
      assert overlay.detail_actions.customer_modal.required_fields == [:customer_id, :full_name]
      assert overlay.detail_actions.customer_modal.payload.title == "Customer {{full_name}}"
    end
  end

  describe "domain registry macros" do
    test "builds joins and schemas from DSL" do
      defmodule TestJoinSchemaOverlay do
        use Selecto.Config.OverlayDSL

        defschema(:initiative, %{
          source_table: "initiatives",
          columns: %{id: %{type: :integer}, name: %{type: :string}}
        })

        defjoin(:initiative, %{
          type: :left,
          schema: :initiative,
          owner_key: :initiative_id,
          related_key: :id
        })
      end

      overlay = TestJoinSchemaOverlay.overlay()

      assert overlay.schemas.initiative.source_table == "initiatives"
      assert overlay.schemas.initiative.columns.name.type == :string
      assert overlay.joins.initiative.type == :left
      assert overlay.joins.initiative.owner_key == :initiative_id
    end

    test "builds root source associations from DSL" do
      defmodule TestSourceAssociationOverlay do
        use Selecto.Config.OverlayDSL

        defsource_assoc(:bundle_parent_load, %{
          queryable: :bundle_parent_load,
          field: :bundle_parent_load,
          owner_key: :bundle_parent_id,
          related_key: :id
        })
      end

      overlay = TestSourceAssociationOverlay.overlay()

      assert overlay.source.associations.bundle_parent_load.queryable == :bundle_parent_load
      assert overlay.source.associations.bundle_parent_load.field == :bundle_parent_load
      assert overlay.source.associations.bundle_parent_load.owner_key == :bundle_parent_id
      assert overlay.source.associations.bundle_parent_load.related_key == :id
    end

    test "builds schema associations from DSL" do
      defmodule TestSchemaAssociationOverlay do
        use Selecto.Config.OverlayDSL

        defschema(:bundle_parent_load, %{
          source_table: "loads",
          columns: %{id: %{type: :integer}}
        })

        defschema_assoc(:bundle_parent_load, :split_parent_load, %{
          queryable: :split_parent_load,
          field: :split_parent_load,
          owner_key: :split_parent_id,
          related_key: :id
        })
      end

      overlay = TestSchemaAssociationOverlay.overlay()

      assert overlay.schemas.bundle_parent_load.source_table == "loads"

      assert overlay.schemas.bundle_parent_load.associations.split_parent_load.queryable ==
               :split_parent_load

      assert overlay.schemas.bundle_parent_load.associations.split_parent_load.field ==
               :split_parent_load
    end
  end

  describe "filter directives" do
    test "name directive" do
      defmodule TestFilterName do
        use Selecto.Config.OverlayDSL

        deffilter "test_filter" do
          name("Test Filter")
        end
      end

      assert TestFilterName.overlay().filters["test_filter"].name == "Test Filter"
    end

    test "type directive" do
      defmodule TestFilterType do
        use Selecto.Config.OverlayDSL

        deffilter "test_filter" do
          type(:boolean)
        end
      end

      assert TestFilterType.overlay().filters["test_filter"].type == :boolean
    end

    test "description directive" do
      defmodule TestFilterDescription do
        use Selecto.Config.OverlayDSL

        deffilter "test_filter" do
          description("This is a test filter")
        end
      end

      assert TestFilterDescription.overlay().filters["test_filter"].description ==
               "This is a test filter"
    end

    test "required directive" do
      defmodule TestFilterRequired do
        use Selecto.Config.OverlayDSL

        deffilter "test_filter" do
          required(true)
        end
      end

      assert TestFilterRequired.overlay().filters["test_filter"].required == true
    end

    test "default directive" do
      defmodule TestFilterDefault do
        use Selecto.Config.OverlayDSL

        deffilter "test_filter" do
          default("active")
        end
      end

      assert TestFilterDefault.overlay().filters["test_filter"].default == "active"
    end

    test "options directive" do
      defmodule TestFilterOptions do
        use Selecto.Config.OverlayDSL

        deffilter "test_filter" do
          options(["option1", "option2", "option3"])
        end
      end

      assert TestFilterOptions.overlay().filters["test_filter"].options == [
               "option1",
               "option2",
               "option3"
             ]
    end

    test "multiple directives in one filter" do
      defmodule TestFilterMultiple do
        use Selecto.Config.OverlayDSL

        deffilter "status" do
          name("Status Filter")
          type(:string)
          description("Filter by status")
          required(false)
          default("active")
          options(["active", "inactive", "pending"])
        end
      end

      filter = TestFilterMultiple.overlay().filters["status"]

      assert filter.name == "Status Filter"
      assert filter.type == :string
      assert filter.description == "Filter by status"
      assert filter.required == false
      assert filter.default == "active"
      assert filter.options == ["active", "inactive", "pending"]
    end
  end

  describe "integration with Selecto.Config.Overlay" do
    test "DSL output merges correctly with base domain" do
      # Create an overlay using DSL
      defmodule TestOverlayIntegration do
        use Selecto.Config.OverlayDSL

        @redactions [:password]

        defcolumn :price do
          label("Product Price")
          format(:currency)
        end

        deffilter "active" do
          name("Active Items")
          type(:boolean)
        end
      end

      # Simulate base domain
      base = %{
        source: %{
          columns: %{
            price: %{type: :decimal},
            name: %{type: :string}
          },
          redact_fields: []
        },
        filters: %{}
      }

      # Merge with overlay
      merged = Selecto.Config.Overlay.merge(base, TestOverlayIntegration.overlay())

      # Verify merge results
      assert merged.source.columns.price.type == :decimal
      assert merged.source.columns.price.label == "Product Price"
      assert merged.source.columns.price.format == :currency
      assert :password in merged.source.redact_fields
      assert merged.filters["active"].name == "Active Items"
    end

    test "DSL source associations merge correctly with base domain" do
      defmodule TestSourceAssociationIntegration do
        use Selecto.Config.OverlayDSL

        defsource_assoc(:bundle_parent_load, %{
          queryable: :bundle_parent_load,
          field: :bundle_parent_load,
          owner_key: :bundle_parent_id,
          related_key: :id
        })
      end

      base = %{
        source: %{
          columns: %{},
          redact_fields: [],
          associations: %{
            customer: %{
              queryable: :customer,
              field: :customer,
              owner_key: :customer_id,
              related_key: :id
            }
          }
        },
        filters: %{}
      }

      merged = Selecto.Config.Overlay.merge(base, TestSourceAssociationIntegration.overlay())

      assert merged.source.associations.customer.queryable == :customer

      assert merged.source.associations.bundle_parent_load.queryable == :bundle_parent_load
      assert merged.source.associations.bundle_parent_load.owner_key == :bundle_parent_id
    end

    test "DSL schema associations merge correctly with base domain" do
      defmodule TestSchemaAssociationIntegration do
        use Selecto.Config.OverlayDSL

        defschema_assoc(:bundle_parent_load, :split_parent_load, %{
          queryable: :split_parent_load,
          field: :split_parent_load,
          owner_key: :split_parent_id,
          related_key: :id
        })
      end

      base = %{
        source: %{columns: %{}, redact_fields: [], associations: %{}},
        schemas: %{
          bundle_parent_load: %{
            source_table: "loads",
            associations: %{
              customer: %{
                queryable: :customer,
                field: :customer,
                owner_key: :customer_id,
                related_key: :id
              }
            }
          }
        },
        filters: %{}
      }

      merged = Selecto.Config.Overlay.merge(base, TestSchemaAssociationIntegration.overlay())

      assert merged.schemas.bundle_parent_load.source_table == "loads"
      assert merged.schemas.bundle_parent_load.associations.customer.queryable == :customer

      assert merged.schemas.bundle_parent_load.associations.split_parent_load.queryable ==
               :split_parent_load
    end
  end

  describe "empty overlay" do
    test "generates empty overlay when no definitions" do
      defmodule TestOverlayEmpty do
        use Selecto.Config.OverlayDSL
      end

      overlay = TestOverlayEmpty.overlay()

      assert overlay.columns == %{}
      assert overlay.filters == %{}
      assert overlay.detail_actions == %{}
      assert overlay.source == %{associations: %{}}
      assert overlay.redact_fields == []
    end
  end

  describe "real-world examples" do
    test "e-commerce product overlay" do
      defmodule ProductOverlay do
        use Selecto.Config.OverlayDSL

        @redactions [:cost_price, :supplier_id]

        defcolumn :price do
          label("Retail Price")
          format(:currency)
          precision(2)
          aggregate_functions([:sum, :avg, :min, :max])
          sortable(true)
        end

        defcolumn :stock_quantity do
          label("Stock")
          aggregate_functions([:sum, :count])
        end

        defcolumn :is_active do
          label("Active")
          format(:yes_no)
        end

        deffilter "price_range" do
          name("Price Range")
          type(:string)
          description("Filter by price range (e.g., '10.00-50.00')")
        end

        deffilter "in_stock" do
          name("In Stock")
          type(:boolean)
          description("Show only items with stock > 0")
          default(true)
        end
      end

      overlay = ProductOverlay.overlay()

      assert map_size(overlay.columns) == 3
      assert map_size(overlay.filters) == 2
      assert length(overlay.redact_fields) == 2
      assert overlay.columns[:price].format == :currency
      assert overlay.filters["in_stock"].default == true
    end
  end
end
