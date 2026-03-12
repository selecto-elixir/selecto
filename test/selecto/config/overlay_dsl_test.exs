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
