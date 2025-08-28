defmodule Selecto.EnhancedJoinsIntegrationTest do
  use ExUnit.Case, async: true
  
  alias Selecto.Builder.Sql
  
  describe "enhanced joins SQL generation integration" do
    test "builds SQL with self-join successfully" do
      # Create a domain with self-join configuration
      domain = %{
        name: "Employees",
        source: %{
          source_table: "employees",
          primary_key: :id,
          fields: [:id, :name, :manager_id],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            manager_id: %{type: :integer}
          },
          associations: %{
            manager: %{
              queryable: :employees,
              field: :manager,
              owner_key: :manager_id,
              related_key: :id
            }
          }
        },
        schemas: %{
          employees: %{
            source_table: "employees",
            primary_key: :id,
            fields: [:id, :name, :manager_id],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              manager_id: %{type: :integer}
            },
            associations: %{}
          }
        },
        joins: %{
          manager: %{
            type: :self_join,
            self_key: :manager_id,
            target_key: :id,
            alias: "mgr",
            condition_type: :left
          }
        }
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      selecto = Selecto.select(selecto, ["name", "manager[name]"])
      
      {sql, _aliases, _params} = Sql.build(selecto, [])
      
      assert sql =~ "LEFT JOIN employees mgr"
      assert sql =~ "selecto_root.manager_id = mgr.id"
    end
    
    test "builds SQL with lateral join successfully" do
      # Create a domain with lateral join configuration  
      domain = %{
        name: "Customers",
        source: %{
          source_table: "customers",
          primary_key: :id,
          fields: [:id, :name, :email],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            email: %{type: :string}
          },
          associations: %{
            recent_orders: %{
              queryable: :orders,
              field: :recent_orders,
              owner_key: :id,
              related_key: :customer_id
            }
          }
        },
        schemas: %{
          orders: %{
            source_table: "orders",
            primary_key: :id,
            fields: [:id, :customer_id, :total, :created_at],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              customer_id: %{type: :integer},
              total: %{type: :decimal},
              created_at: %{type: :utc_datetime}
            },
            associations: %{}
          }
        },
        joins: %{
          recent_orders: %{
            type: :lateral_join,
            lateral_query: "SELECT * FROM orders o WHERE o.customer_id = customers.id ORDER BY o.created_at DESC LIMIT 5",
            alias: "recent"
          }
        }
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      selecto = Selecto.select(selecto, ["name", "recent_orders[total]"])
      
      {sql, _aliases, _params} = Sql.build(selecto, [])
      
      assert sql =~ "LEFT JOIN LATERAL"
      assert sql =~ "SELECT * FROM orders o WHERE o.customer_id = customers.id ORDER BY o.created_at DESC LIMIT 5"
      assert sql =~ "recent ON true"
    end
    
    test "builds SQL with cross join successfully" do
      domain = %{
        name: "Products",
        source: %{
          source_table: "products",
          primary_key: :id,
          fields: [:id, :name, :price],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            price: %{type: :decimal}
          },
          associations: %{
            options: %{
              queryable: :product_options,
              field: :options,
              owner_key: :id,
              related_key: :product_id
            }
          }
        },
        schemas: %{
          product_options: %{
            source_table: "product_options",
            primary_key: :id,
            fields: [:id, :name, :value],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              value: %{type: :string}
            },
            associations: %{}
          }
        },
        joins: %{
          options: %{
            type: :cross_join,
            alias: "opts"
          }
        }
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      selecto = Selecto.select(selecto, ["name", "options[name]"])
      
      {sql, _aliases, _params} = Sql.build(selecto, [])
      
      assert sql =~ "CROSS JOIN product_options opts"
    end
    
    test "builds SQL with full outer join successfully" do
      domain = %{
        name: "Accounts",
        source: %{
          source_table: "accounts",
          primary_key: :id,
          fields: [:id, :account_number, :balance],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            account_number: %{type: :string},
            balance: %{type: :decimal}
          },
          associations: %{
            transactions: %{
              queryable: :transactions,
              field: :transactions,
              owner_key: :id,
              related_key: :account_id
            }
          }
        },
        schemas: %{
          transactions: %{
            source_table: "transactions",
            primary_key: :id,
            fields: [:id, :account_id, :amount, :type],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              account_id: %{type: :integer},
              amount: %{type: :decimal},
              type: %{type: :string}
            },
            associations: %{}
          }
        },
        joins: %{
          transactions: %{
            type: :full_outer_join,
            left_key: :id,
            right_key: :account_id,
            alias: "trans"
          }
        }
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      selecto = Selecto.select(selecto, ["account_number", "transactions[amount]"])
      
      {sql, _aliases, _params} = Sql.build(selecto, [])
      
      assert sql =~ "FULL OUTER JOIN transactions trans"
      assert sql =~ "selecto_root.id = trans.account_id"
    end
    
    test "builds SQL with conditional join successfully" do
      domain = %{
        name: "Orders",
        source: %{
          source_table: "orders",
          primary_key: :id,
          fields: [:id, :total, :created_at],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            total: %{type: :decimal},
            created_at: %{type: :utc_datetime}
          },
          associations: %{
            discounts: %{
              queryable: :discounts,
              field: :discounts,
              owner_key: :id,
              related_key: :applicable_to
            }
          }
        },
        schemas: %{
          discounts: %{
            source_table: "discounts",
            primary_key: :id,
            fields: [:id, :name, :minimum_amount, :valid_from, :valid_to],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              minimum_amount: %{type: :decimal},
              valid_from: %{type: :date},
              valid_to: %{type: :date}
            },
            associations: %{}
          }
        },
        joins: %{
          discounts: %{
            type: :conditional_join,
            conditions: [
              {:field_comparison, "orders.total", :gte, "discounts.minimum_amount"},
              {:date_range, "orders.created_at", "discounts.valid_from", "discounts.valid_to"}
            ],
            condition_type: :left,
            alias: "disc"
          }
        }
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      selecto = Selecto.select(selecto, ["total", "discounts[name]"])
      
      {sql, _aliases, _params} = Sql.build(selecto, [])
      
      assert sql =~ "LEFT JOIN discounts disc"
      assert sql =~ "orders.total >= discounts.minimum_amount"
      assert sql =~ "orders.created_at BETWEEN discounts.valid_from AND discounts.valid_to"
    end
    
    test "fallback to basic join when enhanced join fails" do
      # Test that malformed enhanced join configuration falls back to basic join
      domain = %{
        name: "Test",
        source: %{
          source_table: "test_table",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          },
          associations: %{
            related: %{
              queryable: :related_table,
              field: :related,
              owner_key: :id,
              related_key: :test_id
            }
          }
        },
        schemas: %{
          related_table: %{
            source_table: "related_table",
            primary_key: :id,
            fields: [:id, :test_id, :value],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              test_id: %{type: :integer},
              value: %{type: :string}
            },
            associations: %{}
          }
        },
        joins: %{
          related: %{
            type: :unsupported_join_type  # This should cause fallback
          }
        }
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      selecto = Selecto.select(selecto, ["name", "related[value]"])
      
      {sql, _aliases, _params} = Sql.build(selecto, [])
      
      # Should fallback to basic left join
      assert sql =~ "left join related_table related"
    end
  end
  
  describe "field resolution integration" do
    test "enhanced field resolution works with joins" do
      domain = %{
        name: "Users",
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name, :email],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            email: %{type: :string}
          },
          associations: %{
            posts: %{
              queryable: :posts,
              field: :posts,
              owner_key: :id,
              related_key: :user_id
            }
          }
        },
        schemas: %{
          posts: %{
            source_table: "posts",
            primary_key: :id,
            fields: [:id, :user_id, :title, :content],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              user_id: %{type: :integer},
              title: %{type: :string},
              content: %{type: :text}
            },
            associations: %{}
          }
        },
        joins: %{
          posts: %{}
        }
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      
      # Test basic field resolution
      {:ok, field_info} = Selecto.resolve_field(selecto, "name")
      assert field_info.name == "name"
      assert field_info.source_join == :selecto_root
      
      # Test qualified field resolution  
      {:ok, field_info} = Selecto.resolve_field(selecto, "posts.title")
      assert field_info.name == "title"
      assert field_info.source_join == :posts
      
      # Test field suggestions
      suggestions = Selecto.field_suggestions(selecto, "con")
      assert "posts.content" in suggestions
      
      # Test available fields
      available_fields = Selecto.available_fields(selecto)
      assert Map.has_key?(available_fields, "name")
      assert Map.has_key?(available_fields, "posts.title")
    end
    
    test "backwards compatibility with legacy field function" do
      domain = %{
        name: "Users",
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      selecto = Selecto.configure(domain, :mock_connection, validate: false)
      
      # Legacy field function should still work
      field_info = Selecto.field(selecto, "name")
      assert field_info[:name] == "name"
      assert field_info[:type] == :string
    end
  end
  
  describe "error handling integration" do
    test "SQL generation handles join configuration errors gracefully" do
      # Test with invalid enhanced join configuration
      domain = %{
        name: "Test",
        source: %{
          source_table: "test",
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{
            bad_join: %{
              queryable: :bad_table,
              field: :bad_join,
              owner_key: :id,
              related_key: :test_id
            }
          }
        },
        schemas: %{
          bad_table: %{
            source_table: "bad_table",
            primary_key: :id,
            fields: [:id, :test_id],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              test_id: %{type: :integer}
            },
            associations: %{}
          }
        },
        joins: %{
          bad_join: %{
            type: :lateral_join  # Missing required lateral_query
          }
        }
      }
      
      # Should raise error during configuration due to validation
      assert_raise ArgumentError, fn ->
        selecto = Selecto.configure(domain, :mock_connection, validate: false)
        _sql = Selecto.to_sql(selecto)
      end
    end
  end
end