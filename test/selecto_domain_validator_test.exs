defmodule Selecto.DomainValidatorTest do
  use ExUnit.Case
  alias Selecto.DomainValidator
  alias Selecto.DomainValidator.ValidationError

  describe "validate_domain/1" do
    test "validates successful domain configuration" do
      valid_domain = %{
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
            fields: [:id, :title, :user_id],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              title: %{type: :string},
              user_id: %{type: :integer}
            },
            associations: %{}
          }
        },
        joins: %{
          posts: %{type: :left, name: "posts"}
        },
        name: "TestDomain"
      }

      assert DomainValidator.validate_domain(valid_domain) == :ok
    end

    test "accepts valid query_members configuration" do
      domain_with_query_members = %{
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
        joins: %{},
        name: "Orders",
        query_members: %{
          ctes: %{
            delivered_orders: %{
              query: fn _selecto -> %Selecto{} end,
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
              query: fn _selecto -> %Selecto{} end,
              type: :inner,
              on: [%{left: "id", right: "order_id"}]
            }
          },
          laterals: %{
            explode_tags: %{
              source: {:unnest, "\"selecto_root\".\"tags\""},
              join_type: :left,
              as: "tag_rows"
            }
          },
          unnests: %{
            tag_list: %{
              array_field: "tags",
              as: "tag",
              ordinality: "tag_position"
            }
          }
        }
      }

      assert DomainValidator.validate_domain(domain_with_query_members) == :ok
    end

    test "accepts valid function registry configuration" do
      domain_with_functions = %{
        source: %{
          source_table: "products",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{id: %{type: :integer}, name: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        functions: %{
          "similarity" => %{
            kind: :scalar,
            sql_name: "public.similarity",
            args: [
              %{name: :left, type: :string, source: :selector},
              %{name: :right, type: :string, source: :value}
            ],
            returns: :float,
            allowed_in: [:select, :order_by]
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

      assert DomainValidator.validate_domain(domain_with_functions) == :ok
    end

    test "accepts view-backed source metadata" do
      view_domain = %{
        source: %{
          source_table: "reporting.active_customers",
          primary_key: :customer_id,
          source_kind: :view,
          readonly: true,
          fields: [:customer_id, :name],
          redact_fields: [],
          columns: %{customer_id: %{type: :integer}, name: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        name: "Active Customers"
      }

      assert DomainValidator.validate_domain(view_domain) == :ok
    end

    test "validates invalid source metadata for views support" do
      invalid_domain = %{
        source: %{
          source_table: "reporting.active_customers",
          primary_key: :customer_id,
          source_kind: :report,
          readonly: :yes,
          fields: [:customer_id, :name],
          redact_fields: [],
          columns: %{customer_id: %{type: :integer}, name: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        name: "Active Customers"
      }

      assert {:error, errors} = DomainValidator.validate_domain(invalid_domain)

      assert {:source_invalid_source_kind, :report} in errors
      assert {:source_invalid_readonly, :yes} in errors
    end

    test "validates invalid function registry configuration" do
      invalid_domain = %{
        source: %{
          source_table: "products",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{id: %{type: :integer}, name: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        functions: %{
          "bad_kind" => %{kind: :bogus, sql_name: "public.bad", returns: :string},
          "bad_sql" => %{kind: :scalar, sql_name: "public.bad()", returns: :string},
          "bad_predicate" => %{kind: :predicate, sql_name: "public.bad_pred", returns: :string},
          "bad_table" => %{kind: :table, sql_name: "public.bad_table", returns: :integer},
          "bad_args" => %{
            kind: :scalar,
            sql_name: "public.bad_args",
            args: [%{name: :x, type: :string, source: :bogus}],
            returns: :string
          }
        }
      }

      assert {:error, errors} = DomainValidator.validate_domain(invalid_domain)

      assert Enum.any?(errors, fn
               {:functions_invalid, {"bad_kind", _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:functions_invalid, {"bad_sql", _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:functions_invalid, {"bad_predicate", _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:functions_invalid, {"bad_table", _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:functions_invalid, {"bad_args", _message}} -> true
               _ -> false
             end)
    end

    test "validates invalid query_members configuration" do
      invalid_domain = %{
        source: %{
          source_table: "orders",
          primary_key: :id,
          fields: [:id, :status],
          redact_fields: [],
          columns: %{id: %{type: :integer}, status: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        query_members: %{
          ctes: %{
            # Missing :query/:query_builder function
            bad_cte: %{columns: ["id"]}
          },
          values: %{
            # rows should be a list
            bad_values: %{rows: :not_a_list}
          },
          subqueries: %{
            # query should be function
            bad_subquery: %{query: :not_a_function, type: :bogus}
          },
          laterals: %{
            bad_lateral: %{source: 123, join_type: :bogus}
          },
          unnests: %{
            bad_unnest: %{array_field: nil, ordinality: 42}
          }
        }
      }

      assert {:error, errors} = DomainValidator.validate_domain(invalid_domain)

      assert Enum.any?(errors, fn
               {:query_members_invalid, {:ctes, :bad_cte, _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:query_members_invalid, {:values, :bad_values, _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:query_members_invalid, {:subqueries, :bad_subquery, _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:query_members_invalid, {:laterals, :bad_lateral, _message}} -> true
               _ -> false
             end)

      assert Enum.any?(errors, fn
               {:query_members_invalid, {:unnests, :bad_unnest, _message}} -> true
               _ -> false
             end)
    end

    test "accepts valid detail actions configuration" do
      domain_with_detail_actions = %{
        source: %{
          source_table: "customers",
          primary_key: :id,
          fields: [:id, :customer_id, :full_name],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            customer_id: %{type: :integer},
            full_name: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        name: "Customers",
        detail_actions: %{
          customer_modal: %{
            name: "Customer Modal",
            type: :modal,
            required_fields: [:customer_id, :full_name],
            payload: %{title: "Customer {{full_name}}"}
          },
          customer_profile: %{
            name: "Customer Profile",
            type: :external_link,
            required_fields: [:customer_id],
            payload: %{url_template: "https://example.test/customers/{{customer_id}}"}
          },
          customer_preview: %{
            name: "Customer Preview",
            type: :iframe_modal,
            required_fields: [:customer_id],
            payload: %{url_template: "https://example.test/customers/{{customer_id}}/preview"}
          },
          customer_card: %{
            name: "Customer Card",
            type: :live_component,
            required_fields: [:customer_id, :full_name],
            payload: %{
              module: MyApp.CustomerCardComponent,
              assigns: %{customer_id: {:field, "customer_id"}}
            }
          }
        }
      }

      assert DomainValidator.validate_domain(domain_with_detail_actions) == :ok
    end

    test "validates invalid detail actions configuration" do
      invalid_domain = %{
        source: %{
          source_table: "customers",
          primary_key: :id,
          fields: [:id, :customer_id],
          redact_fields: [],
          columns: %{id: %{type: :integer}, customer_id: %{type: :integer}},
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        name: "Customers",
        detail_actions: %{
          bad_link: %{
            name: "Bad Link",
            type: :external_link,
            required_fields: [:missing_field],
            payload: %{}
          },
          bad_iframe: %{
            name: "Bad Iframe",
            type: :iframe_modal,
            payload: %{}
          },
          bad_component: %{
            name: "Bad Component",
            type: :live_component,
            payload: %{}
          },
          bad_type: %{
            name: "Bad Type",
            type: :made_up,
            payload: %{}
          }
        }
      }

      assert {:error, errors} = DomainValidator.validate_domain(invalid_domain)

      assert Enum.any?(errors, fn
               {:detail_actions_invalid,
                {:bad_link, "external_link actions require payload.url_template"}} ->
                 true

               {:detail_actions_invalid,
                {:bad_iframe, "iframe_modal actions require payload.url_template"}} ->
                 true

               {:detail_actions_invalid,
                {:bad_component, "live_component actions require payload.module"}} ->
                 true

               _ ->
                 false
             end)

      assert Enum.any?(errors, fn
               {:detail_actions_invalid,
                {:bad_link,
                 "required field 'missing_field' was not found in domain configuration"}} ->
                 true

               _ ->
                 false
             end)

      assert Enum.any?(errors, fn
               {:detail_actions_invalid, {:bad_type, _message}} -> true
               _ -> false
             end)
    end

    test "validates missing required keys" do
      invalid_domain = %{
        # Missing :source and :schemas
        joins: %{}
      }

      assert {:error, [{:missing_required_keys, [:source, :schemas]}]} =
               DomainValidator.validate_domain(invalid_domain)
    end

    test "validates schema missing required keys" do
      invalid_domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{}
        },
        schemas: %{
          posts: %{
            # Missing required keys
            source_table: "posts"
            # Missing: primary_key, fields, columns
          }
        },
        name: "TestDomain"
      }

      assert {:error, errors} = DomainValidator.validate_domain(invalid_domain)

      assert Enum.any?(errors, fn
               {:schema_missing_keys, {:posts, missing_keys}} ->
                 [:primary_key, :fields, :columns] -- missing_keys == []

               _ ->
                 false
             end)
    end

    test "validates schema fields have column definitions" do
      invalid_domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{
            id: %{type: :integer}
            # Missing :name column definition
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{},
        name: "TestDomain"
      }

      assert {:error, [source_missing_column_defs: [:name]]} =
               DomainValidator.validate_domain(invalid_domain)
    end

    test "validates association queryable references" do
      invalid_domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{
            posts: %{
              # Invalid reference
              queryable: :nonexistent_schema,
              field: :posts,
              owner_key: :id,
              related_key: :user_id
            }
          }
        },
        schemas: %{},
        joins: %{},
        name: "TestDomain"
      }

      assert {:error, errors} = DomainValidator.validate_domain(invalid_domain)

      assert Enum.any?(errors, fn
               {:association_invalid_queryable, {:source, :posts, :nonexistent_schema}} -> true
               _ -> false
             end)
    end

    test "validates join references existing associations" do
      invalid_domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{}
        },
        schemas: %{},
        joins: %{
          # No such association
          nonexistent_association: %{type: :left, name: "nonexistent_association"}
        }
      }

      assert {:error, errors} = DomainValidator.validate_domain(invalid_domain)

      assert Enum.any?(errors, fn
               {:join_missing_association, {:selecto_root, :nonexistent_association}} -> true
               _ -> false
             end)
    end

    test "detects simple join dependency cycle" do
      # Create a simpler cycle: posts -> comments -> posts
      cyclic_domain = %{
        source: %{
          source_table: "posts",
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{
            comments: %{
              queryable: :comments,
              field: :comments,
              owner_key: :id,
              related_key: :post_id
            }
          }
        },
        schemas: %{
          comments: %{
            source_table: "comments",
            primary_key: :id,
            fields: [:id, :post_id],
            redact_fields: [],
            columns: %{id: %{type: :integer}, post_id: %{type: :integer}},
            associations: %{
              parent_post: %{
                queryable: :posts,
                field: :post,
                owner_key: :post_id,
                related_key: :id
              }
            }
          },
          posts: %{
            source_table: "posts",
            primary_key: :id,
            fields: [:id],
            redact_fields: [],
            columns: %{id: %{type: :integer}},
            associations: %{
              comments: %{
                queryable: :comments,
                field: :comments,
                owner_key: :id,
                related_key: :post_id
              }
            }
          }
        },
        joins: %{
          comments: %{
            type: :left,
            name: "comments",
            joins: %{
              parent_post: %{
                type: :left,
                name: "parent_post",
                joins: %{
                  # This creates the cycle: comments -> parent_post -> comments
                  comments: %{type: :left, name: "nested_comments"}
                }
              }
            }
          }
        },
        name: "TestDomain"
      }

      assert {:error, errors} = DomainValidator.validate_domain(cyclic_domain)

      assert Enum.any?(errors, fn
               {:join_cycle_detected, _cycle} -> true
               _ -> false
             end)
    end

    test "validates dimension join type has required dimension key" do
      dimension_domain = %{
        source: %{
          source_table: "orders",
          primary_key: :id,
          fields: [:id, :customer_id],
          redact_fields: [],
          columns: %{id: %{type: :integer}, customer_id: %{type: :integer}},
          associations: %{
            customer: %{
              queryable: :customers,
              field: :customer,
              owner_key: :customer_id,
              related_key: :id
            }
          }
        },
        schemas: %{
          customers: %{
            source_table: "customers",
            primary_key: :id,
            fields: [:id, :name],
            redact_fields: [],
            columns: %{id: %{type: :integer}, name: %{type: :string}},
            associations: %{}
          }
        },
        joins: %{
          customer: %{
            type: :dimension,
            name: "customer"
            # Missing required :dimension key
          }
        },
        name: "TestDomain"
      }

      assert {:error, errors} = DomainValidator.validate_domain(dimension_domain)

      assert Enum.any?(errors, fn
               {:advanced_join_missing_key, {:customer, :dimension, _message}} -> true
               _ -> false
             end)
    end

    test "validates hierarchical closure table join has required keys" do
      hierarchical_domain = %{
        source: %{
          source_table: "categories",
          primary_key: :id,
          fields: [:id, :parent_id],
          redact_fields: [],
          columns: %{id: %{type: :integer}, parent_id: %{type: :integer}},
          associations: %{
            parent: %{
              queryable: :categories,
              field: :parent,
              owner_key: :parent_id,
              related_key: :id
            }
          }
        },
        schemas: %{
          categories: %{
            source_table: "categories",
            primary_key: :id,
            fields: [:id, :parent_id, :name],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              parent_id: %{type: :integer},
              name: %{type: :string}
            },
            associations: %{}
          }
        },
        joins: %{
          parent: %{
            type: :hierarchical,
            hierarchy_type: :closure_table,
            name: "parent"
            # Missing required keys: closure_table, ancestor_field, descendant_field
          }
        },
        name: "TestDomain"
      }

      assert {:error, errors} = DomainValidator.validate_domain(hierarchical_domain)

      assert Enum.any?(errors, fn
               {:advanced_join_missing_key, {:parent, missing_keys, _message}}
               when is_list(missing_keys) ->
                 [:closure_table, :ancestor_field, :descendant_field] -- missing_keys == []

               _ ->
                 false
             end)
    end

    test "validates snowflake dimension has normalization joins" do
      snowflake_domain = %{
        source: %{
          source_table: "sales",
          primary_key: :id,
          fields: [:id, :product_id],
          redact_fields: [],
          columns: %{id: %{type: :integer}, product_id: %{type: :integer}},
          associations: %{
            product: %{
              queryable: :products,
              field: :product,
              owner_key: :product_id,
              related_key: :id
            }
          }
        },
        schemas: %{
          products: %{
            source_table: "products",
            primary_key: :id,
            fields: [:id, :name, :category_id],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              name: %{type: :string},
              category_id: %{type: :integer}
            },
            associations: %{}
          }
        },
        joins: %{
          product: %{
            type: :snowflake_dimension,
            # Empty list should trigger validation error
            normalization_joins: [],
            name: "product"
          }
        },
        name: "TestDomain"
      }

      assert {:error, errors} = DomainValidator.validate_domain(snowflake_domain)

      assert Enum.any?(errors, fn
               {:advanced_join_missing_key, {:product, :normalization_joins, _message}} -> true
               _ -> false
             end)
    end
  end

  describe "validate_domain!/1" do
    test "raises ValidationError on invalid domain" do
      invalid_domain =
        %{
          # Missing required keys
        }

      assert_raise ValidationError, ~r/Missing required domain keys/, fn ->
        DomainValidator.validate_domain!(invalid_domain)
      end
    end

    test "returns :ok on valid domain" do
      valid_domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }

      assert DomainValidator.validate_domain!(valid_domain) == :ok
    end
  end

  describe "error formatting" do
    test "formats various error types correctly" do
      # Test that format_errors produces readable messages
      errors = [
        {:missing_required_keys, [:source, :schemas]},
        {:join_cycle_detected, [:a, :b, :c]},
        {:association_invalid_queryable, {:source, :posts, :nonexistent}}
      ]

      formatted = DomainValidator.format_errors(errors)

      assert formatted =~ "Missing required domain keys: source, schemas"
      assert formatted =~ "Join dependency cycle detected: a -> b -> c -> a"

      assert formatted =~
               "Association 'posts' in schema 'source' references invalid queryable 'nonexistent'"
    end
  end

  describe "integration with Selecto.configure/3" do
    test "validates domain when validate: true option is passed" do
      # Missing required keys
      invalid_domain = %{}

      assert_raise ValidationError, fn ->
        Selecto.configure(invalid_domain, :mock_connection, validate: true)
      end
    end

    test "skips validation when validate: false or not specified" do
      # Missing required keys - but validation is skipped
      invalid_domain = %{}

      # This should not raise a ValidationError, but will likely fail later during configure_domain
      # with a different error type (like FunctionClauseError)
      assert_raise FunctionClauseError, fn ->
        Selecto.configure(invalid_domain, :mock_connection, validate: false)
      end
    end

    test "successful validation allows normal configure" do
      valid_domain = %{
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
        joins: %{},
        name: "TestDomain"
      }

      selecto = Selecto.configure(valid_domain, :mock_connection, validate: true)
      assert %Selecto{} = selecto
      assert selecto.domain == valid_domain
    end
  end
end
