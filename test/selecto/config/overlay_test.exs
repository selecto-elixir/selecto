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

    test "merges functions deeply" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        functions: %{
          "similarity" => %{
            kind: :scalar,
            sql_name: "public.similarity",
            args: [%{name: :left, type: :string, source: :selector}],
            returns: :float,
            allowed_in: [:select]
          }
        }
      }

      overlay = %{
        functions: %{
          "similarity" => %{
            allowed_in: [:select, :order_by],
            args: [
              %{name: :left, type: :string, source: :selector},
              %{name: :right, type: :string, source: :value}
            ]
          },
          "matches_name" => %{
            kind: :predicate,
            sql_name: "public.matches_name",
            args: [
              %{name: :name, type: :string, source: :selector},
              %{name: :pattern, type: :string, source: :value}
            ],
            returns: :boolean,
            allowed_in: [:filter]
          }
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.functions["similarity"].sql_name == "public.similarity"
      assert result.functions["similarity"].allowed_in == [:select, :order_by]
      assert length(result.functions["similarity"].args) == 2
      assert result.functions["matches_name"].kind == :predicate
    end

    test "merges detail actions deeply" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        detail_actions: %{
          customer_modal: %{
            name: "Customer Modal",
            type: :modal,
            payload: %{title: "Customer"}
          }
        }
      }

      overlay = %{
        detail_actions: %{
          customer_modal: %{payload: %{size: :xl}},
          customer_profile: %{
            name: "Customer Profile",
            type: :external_link,
            payload: %{url_template: "https://example.test/customers/{{customer_id}}"}
          }
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.detail_actions.customer_modal.name == "Customer Modal"
      assert result.detail_actions.customer_modal.payload.title == "Customer"
      assert result.detail_actions.customer_modal.payload.size == :xl
      assert result.detail_actions.customer_profile.type == :external_link
    end

    test "merges query_members deeply" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        query_members: %{
          ctes: %{
            active_orders: %{columns: ["id"], join: [owner_key: :id, related_key: :id]}
          },
          values: %{},
          laterals: %{
            explode_tags: %{source: {:unnest, "\"selecto_root\".\"tags\""}, join_type: :left}
          }
        }
      }

      overlay = %{
        query_members: %{
          ctes: %{
            active_orders: %{columns: ["id", "status"]},
            delayed_orders: %{columns: ["id", "delayed_days"]}
          },
          subqueries: %{
            high_value: %{on: [%{left: "id", right: "customer_id"}]}
          },
          laterals: %{
            explode_tags: %{as: "tag_rows", join_type: :inner}
          },
          unnests: %{
            tag_values: %{array_field: "tags", as: "tag"}
          }
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.query_members.ctes.active_orders.join ==
               [owner_key: :id, related_key: :id]

      assert result.query_members.ctes.active_orders.columns == ["id", "status"]
      assert result.query_members.ctes.delayed_orders.columns == ["id", "delayed_days"]

      assert result.query_members.subqueries.high_value.on == [
               %{left: "id", right: "customer_id"}
             ]

      assert result.query_members.laterals.explode_tags.source ==
               {:unnest, "\"selecto_root\".\"tags\""}

      assert result.query_members.laterals.explode_tags.as == "tag_rows"
      assert result.query_members.laterals.explode_tags.join_type == :inner
      assert result.query_members.unnests.tag_values.array_field == "tags"
    end

    test "merges schemas deeply" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        schemas: %{
          initiative: %{source_table: "initiatives", columns: %{id: %{type: :integer}}}
        }
      }

      overlay = %{
        schemas: %{
          initiative: %{columns: %{name: %{type: :string}}},
          sponsor: %{source_table: "sponsors", columns: %{id: %{type: :integer}}}
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.schemas.initiative.source_table == "initiatives"
      assert result.schemas.initiative.columns.id.type == :integer
      assert result.schemas.initiative.columns.name.type == :string
      assert result.schemas.sponsor.source_table == "sponsors"
    end

    test "merges schema associations deeply" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
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
        }
      }

      overlay = %{
        schemas: %{
          bundle_parent_load: %{
            associations: %{
              split_parent_load: %{
                queryable: :split_parent_load,
                field: :split_parent_load,
                owner_key: :split_parent_id,
                related_key: :id
              }
            }
          }
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.schemas.bundle_parent_load.source_table == "loads"
      assert result.schemas.bundle_parent_load.associations.customer.queryable == :customer

      assert result.schemas.bundle_parent_load.associations.split_parent_load.queryable ==
               :split_parent_load
    end

    test "merges joins deeply" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        joins: %{
          initiative: %{type: :left, owner_key: :initiative_id, related_key: :id}
        }
      }

      overlay = %{
        joins: %{
          initiative: %{name: "initiative_join"},
          sponsor: %{type: :inner, owner_key: :sponsor_id, related_key: :id}
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.joins.initiative.type == :left
      assert result.joins.initiative.owner_key == :initiative_id
      assert result.joins.initiative.name == "initiative_join"
      assert result.joins.sponsor.type == :inner
    end

    test "merges source associations deeply without replacing source" do
      base = %{
        source: %{
          source_table: "loads",
          primary_key: :id,
          fields: [:id, :customer_id],
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
        }
      }

      overlay = %{
        source: %{
          associations: %{
            bundle_parent_load: %{
              queryable: :bundle_parent_load,
              field: :bundle_parent_load,
              owner_key: :bundle_parent_id,
              related_key: :id
            }
          }
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.source.source_table == "loads"
      assert result.source.primary_key == :id
      assert result.source.fields == [:id, :customer_id]
      assert result.source.associations.customer.queryable == :customer
      assert result.source.associations.bundle_parent_load.queryable == :bundle_parent_load
      assert result.source.associations.bundle_parent_load.owner_key == :bundle_parent_id
    end

    test "merges source relationships and choice sources deeply" do
      base = %{
        source: %{columns: %{}, redact_fields: []},
        source_relationships: %{
          customer: %{
            target_domain: :customers,
            source_field: :customer_id,
            target_field: :id,
            filters: []
          }
        },
        choice_sources: %{
          customer_choices: %{
            domain: :customers,
            value_field: :id,
            label_field: :name,
            presentation: %{control: :select}
          }
        }
      }

      overlay = %{
        source_relationships: %{
          customer: %{filters: [%{field: :active, op: :eq, value: true}]},
          assignee: %{
            target_domain: :employees,
            source_field: :assignee_id,
            target_field: :id
          }
        },
        choice_sources: %{
          customer_choices: %{presentation: %{mode: :searchable}},
          assignee_choices: %{
            domain: :employees,
            value_field: :id,
            label_field: :full_name
          }
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.source_relationships.customer.target_domain == :customers

      assert result.source_relationships.customer.filters == [
               %{field: :active, op: :eq, value: true}
             ]

      assert result.source_relationships.assignee.source_field == :assignee_id
      assert result.choice_sources.customer_choices.presentation.control == :select
      assert result.choice_sources.customer_choices.presentation.mode == :searchable
      assert result.choice_sources.assignee_choices.label_field == :full_name
    end

    test "merges redact_fields as union" do
      base = %{
        source: %{
          columns: %{},
          redact_fields: [:password, :secret_key]
        }
      }

      overlay = %{
        # password is duplicate
        redact_fields: [:internal_notes, :password]
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

    test "self-join overlay can add root and alias schema associations safely" do
      base = %{
        source: %{
          source_table: "loads",
          primary_key: :id,
          fields: [:id, :bundle_parent_id, :split_parent_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            bundle_parent_id: %{type: :integer},
            split_parent_id: %{type: :integer}
          },
          associations: %{}
        },
        schemas: %{
          bundle_parent_load: %{
            source_table: "loads",
            primary_key: :id,
            fields: [:id, :split_parent_id],
            redact_fields: [],
            columns: %{id: %{type: :integer}, split_parent_id: %{type: :integer}},
            associations: %{}
          },
          split_parent_load: %{
            source_table: "loads",
            primary_key: :id,
            fields: [:id],
            redact_fields: [],
            columns: %{id: %{type: :integer}},
            associations: %{}
          }
        },
        joins: %{
          bundle_parent_load: %{
            type: :left,
            schema: :bundle_parent_load,
            owner_key: :bundle_parent_id,
            related_key: :id
          },
          split_parent_load: %{
            type: :left,
            schema: :split_parent_load,
            owner_key: :split_parent_id,
            related_key: :id
          }
        }
      }

      overlay = %{
        source: %{
          associations: %{
            bundle_parent_load: %{
              queryable: :bundle_parent_load,
              field: :bundle_parent_load,
              owner_key: :bundle_parent_id,
              related_key: :id
            },
            split_parent_load: %{
              queryable: :split_parent_load,
              field: :split_parent_load,
              owner_key: :split_parent_id,
              related_key: :id
            }
          }
        },
        schemas: %{
          bundle_parent_load: %{
            associations: %{
              split_parent_load: %{
                queryable: :split_parent_load,
                field: :split_parent_load,
                owner_key: :split_parent_id,
                related_key: :id
              }
            }
          }
        }
      }

      result = Overlay.merge(base, overlay)

      assert result.source.source_table == "loads"
      assert result.source.primary_key == :id
      assert result.source.associations.bundle_parent_load.queryable == :bundle_parent_load
      assert result.source.associations.split_parent_load.queryable == :split_parent_load

      assert result.schemas.bundle_parent_load.associations.split_parent_load.queryable ==
               :split_parent_load

      assert result.schemas.split_parent_load.associations == %{}
    end
  end
end
