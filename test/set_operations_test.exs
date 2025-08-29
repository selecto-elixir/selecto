defmodule SetOperationsTest do
  use ExUnit.Case, async: true

  alias Selecto.SetOperations
  alias Selecto.SetOperations.{Spec, Validation}

  describe "Set Operations API" do
    setup do
      # Create basic selecto structs for testing using configure
      domain = %{
        name: "users_domain",
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name, :email, :status],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string}, 
            email: %{type: :string},
            status: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      selecto1 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["name", "email"])
        |> Selecto.filter([{"status", "active"}])
      
      selecto2 = Selecto.configure(domain, [], validate: false)  
        |> Selecto.select(["name", "email"])
        |> Selecto.filter([{"status", "inactive"}])
      
      {:ok, selecto1: selecto1, selecto2: selecto2}
    end

    test "creates UNION operation", %{selecto1: selecto1, selecto2: selecto2} do
      result = Selecto.union(selecto1, selecto2)
      
      set_operations = Map.get(result.set, :set_operations, [])
      assert length(set_operations) == 1
      
      [operation] = set_operations
      assert %Spec{
        operation: :union,
        left_query: ^selecto1,
        right_query: ^selecto2,
        options: %{all: false}
      } = operation
      assert operation.validated == true
    end

    test "creates UNION ALL operation", %{selecto1: selecto1, selecto2: selecto2} do
      result = Selecto.union(selecto1, selecto2, all: true)
      
      [operation] = Map.get(result.set, :set_operations, [])
      assert operation.options.all == true
    end

    test "creates INTERSECT operation", %{selecto1: selecto1, selecto2: selecto2} do
      result = Selecto.intersect(selecto1, selecto2)
      
      [operation] = Map.get(result.set, :set_operations, [])
      assert operation.operation == :intersect
    end

    test "creates EXCEPT operation", %{selecto1: selecto1, selecto2: selecto2} do
      result = Selecto.except(selecto1, selecto2)
      
      [operation] = Map.get(result.set, :set_operations, [])
      assert operation.operation == :except
    end

    test "supports chained set operations", %{selecto1: selecto1, selecto2: selecto2} do
      selecto3 = %Selecto{
        domain: selecto1.domain,
        postgrex_opts: [],
        set: %{
          selected: ["name", "email"],
          filtered: [{"status", "pending"}],
          order_by: [],
          group_by: []
        }
      }
      
      result = selecto1
        |> Selecto.union(selecto2)
        |> Selecto.intersect(selecto3)
      
      set_operations = Map.get(result.set, :set_operations, [])
      assert length(set_operations) == 2
      
      [op1, op2] = set_operations
      assert op1.operation == :union
      assert op2.operation == :intersect
    end
  end

  describe "Schema validation" do
    test "validates compatible schemas" do
      domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:name, :email],
          redact_fields: [],
          columns: %{
            name: %{type: :string},
            email: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      query1 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["name", "email"])
      
      query2 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["name", "email"])
      
      spec = %Spec{
        operation: :union,
        left_query: query1,
        right_query: query2,
        options: %{all: false},
        validated: false
      }
      
      assert {:ok, validated_spec} = Validation.validate_compatibility(spec)
      assert validated_spec.validated == true
    end

    test "rejects queries with different column counts" do
      domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:name, :email],
          redact_fields: [],
          columns: %{
            name: %{type: :string},
            email: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      query1 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["name", "email"])
      
      query2 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["name"])
      
      spec = %Spec{
        operation: :union,
        left_query: query1,
        right_query: query2,
        options: %{all: false},
        validated: false
      }
      
      assert {:error, %Validation.SchemaError{type: :column_count_mismatch}} = 
        Validation.validate_compatibility(spec)
    end

    test "validates compatible numeric types" do
      domain1 = %{
        source: %{
          source_table: "products",
          primary_key: :id,
          fields: [:price, :quantity],
          redact_fields: [],
          columns: %{
            price: %{type: :decimal},
            quantity: %{type: :integer}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      domain2 = %{
        source: %{
          source_table: "orders",
          primary_key: :id,
          fields: [:total, :items],
          redact_fields: [],
          columns: %{
            total: %{type: :float},   # Should be compatible with decimal
            items: %{type: :integer}   # Should be compatible with integer
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      query1 = Selecto.configure(domain1, [], validate: false)
        |> Selecto.select(["price", "quantity"])
      
      query2 = Selecto.configure(domain2, [], validate: false)
        |> Selecto.select(["total", "items"])
      
      spec = %Spec{
        operation: :union,
        left_query: query1,
        right_query: query2,
        options: %{all: false},
        validated: false
      }
      
      assert {:ok, _validated_spec} = Validation.validate_compatibility(spec)
    end

    test "validates compatible string types" do
      domain1 = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:name, :bio],
          redact_fields: [],
          columns: %{
            name: %{type: :string},
            bio: %{type: :text}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      domain2 = %{
        source: %{
          source_table: "authors",
          primary_key: :id,
          fields: [:title, :description],
          redact_fields: [],
          columns: %{
            title: %{type: :text},     # Should be compatible with string
            description: %{type: :string}  # Should be compatible with text
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      query1 = Selecto.configure(domain1, [], validate: false)
        |> Selecto.select(["name", "bio"])
      
      query2 = Selecto.configure(domain2, [], validate: false)
        |> Selecto.select(["title", "description"])
      
      spec = %Spec{
        operation: :union,
        left_query: query1,
        right_query: query2,
        options: %{all: false},
        validated: false
      }
      
      assert {:ok, _validated_spec} = Validation.validate_compatibility(spec)
    end
  end

  describe "SQL generation" do
    setup do
      # Create a simple domain for SQL generation testing
      domain = %{
        source: %{
          source_table: "film",
          primary_key: :film_id,
          fields: [:film_id, :title, :rental_rate, :rating],
          redact_fields: [],
          columns: %{
            film_id: %{type: :integer},
            title: %{type: :string},
            rental_rate: %{type: :decimal},
            rating: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      query1 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.filter([{"rating", "PG"}])
        
      query2 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.filter([{"rating", "G"}])
        
      {:ok, query1: query1, query2: query2}
    end

    test "generates UNION SQL", %{query1: query1, query2: query2} do
      result = Selecto.union(query1, query2)
      {sql, params} = Selecto.to_sql(result)
      
      assert sql =~ "UNION"
      assert not (sql =~ "UNION ALL")
      assert sql =~ "SELECT"
      assert sql =~ "film.title"
      assert sql =~ "film.rental_rate"
      assert params == ["PG", "G"]
    end

    test "generates UNION ALL SQL", %{query1: query1, query2: query2} do
      result = Selecto.union(query1, query2, all: true)
      {sql, params} = Selecto.to_sql(result)
      
      assert sql =~ "UNION ALL"
      assert params == ["PG", "G"]
    end

    test "generates INTERSECT SQL", %{query1: query1, query2: query2} do
      result = Selecto.intersect(query1, query2)
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "INTERSECT"
      assert not (sql =~ "INTERSECT ALL")
    end

    test "generates EXCEPT SQL", %{query1: query1, query2: query2} do
      result = Selecto.except(query1, query2)
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "EXCEPT"
      assert not (sql =~ "EXCEPT ALL")
    end

    test "generates chained set operations SQL", %{query1: query1, query2: query2} do
      query3 = Selecto.configure(query1.domain, [])
        |> Selecto.select(["title", "rental_rate"])
        |> Selecto.filter([{"rating", "R"}])
        
      result = query1
        |> Selecto.union(query2)
        |> Selecto.intersect(query3)
      
      {sql, params} = Selecto.to_sql(result)
      
      assert sql =~ "UNION"
      assert sql =~ "INTERSECT"
      assert params == ["PG", "G", "R"]
    end

    test "handles ORDER BY on set operations", %{query1: query1, query2: query2} do
      result = query1
        |> Selecto.union(query2)
        |> Selecto.order_by([{"title", :asc}])
      
      {sql, _params} = Selecto.to_sql(result)
      
      assert sql =~ "UNION"
      assert sql =~ "ORDER BY"
      assert sql =~ "film.title"
    end
  end

  describe "Error handling" do
    test "raises error for incompatible schemas" do
      domain1 = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:name, :email],
          redact_fields: [],
          columns: %{
            name: %{type: :string},
            email: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      domain2 = %{
        source: %{
          source_table: "products",
          primary_key: :id,
          fields: [:price],
          redact_fields: [],
          columns: %{
            price: %{type: :decimal}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      query1 = Selecto.configure(domain1, [], validate: false)
        |> Selecto.select(["name", "email"])
      
      query2 = Selecto.configure(domain2, [], validate: false)
        |> Selecto.select(["price"])
      
      assert_raise Selecto.SetOperations.Validation.SchemaError, fn ->
        Selecto.union(query1, query2)
      end
    end

    test "handles queries with no selected columns" do
      domain = %{
        source: %{
          source_table: "users",
          primary_key: :id,
          fields: [:name],
          redact_fields: [],
          columns: %{name: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      # We can't test this directly since Selecto.select([]) would be invalid
      # Instead test the validation function directly
      query1 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["name"])
      
      # Create a query with empty selections by manipulating the set
      empty_query = %{query1 | set: Map.put(query1.set, :selected, [])}
      
      query2 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["name"])
      
      assert_raise Selecto.SetOperations.Validation.SchemaError, fn ->
        Selecto.union(empty_query, query2)
      end
    end
  end

  describe "Complex scenarios" do
    setup do
      domain = %{
        source: %{
          source_table: "film",
          primary_key: :film_id,
          fields: [:film_id, :title, :rating],
          redact_fields: [],
          columns: %{
            film_id: %{type: :integer},
            title: %{type: :string},
            rating: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      {:ok, domain: domain}
    end

    test "set operations with literal selections", %{domain: domain} do
      query1 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["title", {:as, {:literal, "film"}, "type"}])
        
      query2 = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["title", {:as, {:literal, "movie"}, "type"}])
        
      result = Selecto.union(query1, query2)
      
      # Should not raise validation error
      assert %Selecto{} = result
      set_ops = Map.get(result.set, :set_operations, [])
      assert length(set_ops) == 1
    end
  end
end