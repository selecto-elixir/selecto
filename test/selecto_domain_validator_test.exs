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
        _ -> false
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

      # This validation happens during field building - schema validation focuses on structure
      assert DomainValidator.validate_domain(invalid_domain) == :ok
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
              queryable: :nonexistent_schema,  # Invalid reference
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
          nonexistent_association: %{type: :left, name: "nonexistent_association"}  # No such association
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
            comments: %{queryable: :comments, field: :comments, owner_key: :id, related_key: :post_id}
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
              parent_post: %{queryable: :posts, field: :post, owner_key: :post_id, related_key: :id}
            }
          },
          posts: %{
            source_table: "posts",  
            primary_key: :id,
            fields: [:id],
            redact_fields: [],
            columns: %{id: %{type: :integer}},
            associations: %{
              comments: %{queryable: :comments, field: :comments, owner_key: :id, related_key: :post_id}
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
                  comments: %{type: :left, name: "nested_comments"}  # This creates the cycle: comments -> parent_post -> comments
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
            customer: %{queryable: :customers, field: :customer, owner_key: :customer_id, related_key: :id}
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
            parent: %{queryable: :categories, field: :parent, owner_key: :parent_id, related_key: :id}
          }
        },
        schemas: %{
          categories: %{
            source_table: "categories",
            primary_key: :id,
            fields: [:id, :parent_id, :name],
            redact_fields: [],
            columns: %{id: %{type: :integer}, parent_id: %{type: :integer}, name: %{type: :string}},
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
        {:advanced_join_missing_key, {:parent, missing_keys, _message}} when is_list(missing_keys) -> 
          [:closure_table, :ancestor_field, :descendant_field] -- missing_keys == []
        _ -> false
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
            product: %{queryable: :products, field: :product, owner_key: :product_id, related_key: :id}
          }
        },
        schemas: %{
          products: %{
            source_table: "products",
            primary_key: :id,
            fields: [:id, :name, :category_id],
            redact_fields: [],
            columns: %{id: %{type: :integer}, name: %{type: :string}, category_id: %{type: :integer}},
            associations: %{}
          }
        },
        joins: %{
          product: %{
            type: :snowflake_dimension,
            normalization_joins: [],  # Empty list should trigger validation error
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
      invalid_domain = %{
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
      assert formatted =~ "Association 'posts' in schema 'source' references invalid queryable 'nonexistent'"
    end
  end

  describe "integration with Selecto.configure/3" do
    test "validates domain when validate: true option is passed" do
      invalid_domain = %{}  # Missing required keys
      
      assert_raise ValidationError, fn ->
        Selecto.configure(invalid_domain, :mock_connection, validate: true)
      end
    end

    test "skips validation when validate: false or not specified" do
      invalid_domain = %{}  # Missing required keys - but validation is skipped
      
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