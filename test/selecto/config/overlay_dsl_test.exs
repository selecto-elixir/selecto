defmodule Selecto.Config.OverlayDSLTest do
  use ExUnit.Case, async: true

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
  end

  describe "empty overlay" do
    test "generates empty overlay when no definitions" do
      defmodule TestOverlayEmpty do
        use Selecto.Config.OverlayDSL
      end

      overlay = TestOverlayEmpty.overlay()

      assert overlay.columns == %{}
      assert overlay.filters == %{}
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
