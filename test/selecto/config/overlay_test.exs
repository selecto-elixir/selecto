defmodule Selecto.Config.OverlayTest do
  use ExUnit.Case, async: true

  alias Selecto.Config.Overlay

  describe "merge/2" do
    test "merges column configurations deeply" do
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

      overlay = %{
        columns: %{
          price: %{
            label: "Product Price",
            format: :currency,
            aggregate_functions: [:sum, :avg]
          }
        }
      }

      result = Overlay.merge(base, overlay)

      # Column should have both base type and overlay properties
      assert result.source.columns.price.type == :decimal
      assert result.source.columns.price.label == "Product Price"
      assert result.source.columns.price.format == :currency
      assert result.source.columns.price.aggregate_functions == [:sum, :avg]

      # Unmodified column should remain unchanged
      assert result.source.columns.name == %{type: :string}
    end

    test "merges filters additively" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        filters: %{
          "base_filter" => %{name: "Base Filter", type: :string}
        }
      }

      overlay = %{
        filters: %{
          "custom_filter" => %{name: "Custom Filter", type: :boolean}
        }
      }

      result = Overlay.merge(base, overlay)

      # Both filters should be present
      assert map_size(result.filters) == 2
      assert result.filters["base_filter"] == %{name: "Base Filter", type: :string}
      assert result.filters["custom_filter"] == %{name: "Custom Filter", type: :boolean}
    end

    test "overlay filter overwrites base filter with same key" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        filters: %{
          "status" => %{name: "Status", type: :string}
        }
      }

      overlay = %{
        filters: %{
          "status" => %{name: "Status Filter", type: :boolean}
        }
      }

      result = Overlay.merge(base, overlay)

      # Overlay should win
      assert result.filters["status"] == %{name: "Status Filter", type: :boolean}
    end

    test "merges redact_fields as union" do
      base = %{
        source: %{
          columns: %{},
          redact_fields: [:password, :secret_key]
        }
      }

      overlay = %{
        redact_fields: [:internal_notes, :password]  # password is duplicate
      }

      result = Overlay.merge(base, overlay)

      # Should have unique union
      assert :password in result.source.redact_fields
      assert :secret_key in result.source.redact_fields
      assert :internal_notes in result.source.redact_fields
      assert length(result.source.redact_fields) == 3
    end

    test "overlay takes precedence for other fields" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        name: "Base Domain",
        custom_field: "base_value"
      }

      overlay = %{
        name: "Overlay Domain",
        custom_field: "overlay_value"
      }

      result = Overlay.merge(base, overlay)

      assert result.name == "Overlay Domain"
      assert result.custom_field == "overlay_value"
    end

    test "handles empty overlay gracefully" do
      base = %{
        source: %{
          columns: %{price: %{type: :decimal}},
          redact_fields: []
        },
        filters: %{}
      }

      result = Overlay.merge(base, %{})

      assert result == base
    end

    test "handles nil overlay gracefully" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        filters: %{}
      }

      result = Overlay.merge(base, nil)

      assert result == base
    end

    test "deeply nested column properties are merged" do
      base = %{
        source: %{
          columns: %{
            metadata: %{
              type: :map,
              nested: %{
                deep: %{
                  value: "base"
                }
              }
            }
          },
          redact_fields: []
        }
      }

      overlay = %{
        columns: %{
          metadata: %{
            label: "Metadata",
            nested: %{
              deep: %{
                value: "overlay",
                new_key: "new"
              }
            }
          }
        }
      }

      result = Overlay.merge(base, overlay)

      # Type should be preserved from base
      assert result.source.columns.metadata.type == :map
      # Label should be added from overlay
      assert result.source.columns.metadata.label == "Metadata"
      # Deep nested value should be from overlay (overlay wins at leaf level)
      assert result.source.columns.metadata.nested.deep.value == "overlay"
      assert result.source.columns.metadata.nested.deep.new_key == "new"
    end
  end

  describe "Integration: Full overlay workflow" do
    test "complete overlay merge workflow" do
      # Simulate base domain configuration
      base_domain = %{
        source: %{
          source_table: "products",
          primary_key: :id,
          fields: [:id, :name, :price, :description],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            price: %{type: :decimal},
            description: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        name: "Products Domain",
        default_selected: [:id, :name, :price],
        filters: %{},
        subfilters: %{},
        window_functions: %{},
        pagination: %{
          default_limit: 50,
          max_limit: 1000
        },
        pivot: %{},
        joins: %{}
      }

      # Simulate user overlay configuration
      user_overlay = %{
        columns: %{
          price: %{
            label: "Product Price",
            format: :currency,
            aggregate_functions: [:sum, :avg, :min, :max]
          },
          description: %{
            label: "Product Description",
            max_length: 100
          }
        },
        filters: %{
          "price_range" => %{
            name: "Price Range",
            type: :string,
            description: "Filter by price range (e.g., '10-100')"
          }
        },
        redact_fields: [:description]
      }

      # Merge them
      merged = Overlay.merge(base_domain, user_overlay)

      # Verify merged result
      # Columns should have both base and overlay properties
      assert merged.source.columns.price.type == :decimal
      assert merged.source.columns.price.label == "Product Price"
      assert merged.source.columns.price.format == :currency

      # Filters should be extended
      assert merged.filters["price_range"] != nil

      # Redaction should be applied
      assert :description in merged.source.redact_fields

      # Base properties should remain
      assert merged.source.source_table == "products"
      assert merged.name == "Products Domain"
      assert merged.default_selected == [:id, :name, :price]
    end

    test "runtime multi-tenant overlay" do
      base = %{
        source: %{
          columns: %{price: %{type: :decimal}},
          redact_fields: []
        },
        filters: %{}
      }

      # Premium tenant gets enhanced column config
      premium_overlay = %{
        columns: %{
          price: %{format: :currency_with_symbol, precision: 2}
        }
      }

      # Basic tenant gets no overlay
      basic_overlay = %{}

      premium_config = Overlay.merge(base, premium_overlay)
      basic_config = Overlay.merge(base, basic_overlay)

      # Premium has enhanced config
      assert premium_config.source.columns.price.format == :currency_with_symbol
      assert premium_config.source.columns.price.precision == 2

      # Basic just has base config
      assert basic_config.source.columns.price == %{type: :decimal}
    end
  end
end
