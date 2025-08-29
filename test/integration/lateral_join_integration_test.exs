defmodule Selecto.Integration.LateralJoinTest do
  use ExUnit.Case, async: true
  
  describe "LATERAL join integration with Selecto API" do
    setup do
      # Create a test domain
      domain = %{
        name: "film_domain",
        source: %{
          source_table: "film",
          primary_key: :film_id,
          fields: [:film_id, :title, :rating, :special_features],
          redact_fields: [],
          columns: %{
            film_id: %{type: :integer},
            title: %{type: :string},
            rating: %{type: :string},
            special_features: %{type: :array}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      rental_domain = %{
        name: "rental_domain",
        source: %{
          source_table: "rental",
          primary_key: :rental_id,
          fields: [:rental_id, :rental_date, :customer_id, :inventory_id],
          redact_fields: [],
          columns: %{
            rental_id: %{type: :integer},
            rental_date: %{type: :utc_datetime},
            customer_id: %{type: :integer},
            inventory_id: %{type: :integer}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      {:ok, domain: domain, rental_domain: rental_domain}
    end
    
    test "adds LATERAL join with table function to query", %{domain: domain} do
      selecto = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["film.title", "features.value"])
        |> Selecto.lateral_join(
          :inner,
          {:unnest, "film.special_features"},
          "features"
        )
      
      # Check that LATERAL join was added to the set
      lateral_joins = Map.get(selecto.set, :lateral_joins, [])
      assert length(lateral_joins) == 1
      
      [lateral_spec] = lateral_joins
      assert lateral_spec.join_type == :inner
      assert lateral_spec.table_function == {:unnest, "film.special_features"}
      assert lateral_spec.alias == "features"
      assert lateral_spec.validated == true
    end
    
    test "generates correct SQL for LATERAL join with table function", %{domain: domain} do
      selecto = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["film.title", "features.value"])
        |> Selecto.lateral_join(
          :inner,
          {:unnest, "film.special_features"},
          "features"
        )
      
      {sql, params} = Selecto.to_sql(selecto)
      
      assert String.contains?(sql, "SELECT")
      assert String.contains?(sql, "film.title")
      assert String.contains?(sql, "FROM film")
      assert String.contains?(sql, "INNER JOIN LATERAL UNNEST(film.special_features) AS features ON true")
      assert params == []
    end
    
    test "generates SQL for LATERAL join with correlated subquery", %{domain: domain, rental_domain: rental_domain} do
      selecto = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["film.title", "rental_counts.rental_count"])
        |> Selecto.lateral_join(
          :left,
          fn _base ->
            Selecto.configure(rental_domain, [], validate: false)
            |> Selecto.select([{:func, "COUNT", ["*"], as: "rental_count"}])
            |> Selecto.filter([{"inventory_id", {:ref, "film.film_id"}}])
            |> Selecto.filter([{"rental_date", {:>, {:func, "CURRENT_DATE - INTERVAL '30 days'"}}}])
          end,
          "rental_counts"
        )
      
      {sql, params} = Selecto.to_sql(selecto)
      
      assert String.contains?(sql, "SELECT")
      assert String.contains?(sql, "film.title")
      assert String.contains?(sql, "rental_counts.rental_count")
      assert String.contains?(sql, "FROM film")
      assert String.contains?(sql, "LEFT JOIN LATERAL (")
      assert String.contains?(sql, "FROM rental")
      assert String.contains?(sql, ") AS rental_counts ON true")
      
      # Should contain the correlation reference and date filter
      # Note: The exact parameter binding may vary based on implementation
      assert is_list(params)
    end
    
    test "generates SQL for LATERAL join with generate_series", %{domain: domain} do
      selecto = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["film.title", "numbers.value"])
        |> Selecto.lateral_join(
          :inner,
          {:function, :generate_series, [1, 10]},
          "numbers"
        )
      
      {sql, params} = Selecto.to_sql(selecto)
      
      assert String.contains?(sql, "INNER JOIN LATERAL GENERATE_SERIES(?, ?) AS numbers ON true")
      assert params == [1, 10]
    end
    
    test "supports multiple LATERAL joins in same query", %{domain: domain} do
      selecto = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["film.title", "features.value", "numbers.value"])
        |> Selecto.lateral_join(
          :inner,
          {:unnest, "film.special_features"},
          "features"
        )
        |> Selecto.lateral_join(
          :left,
          {:function, :generate_series, [1, 5]},
          "numbers"
        )
      
      lateral_joins = Map.get(selecto.set, :lateral_joins, [])
      assert length(lateral_joins) == 2
      
      {sql, params} = Selecto.to_sql(selecto)
      
      assert String.contains?(sql, "INNER JOIN LATERAL UNNEST(film.special_features) AS features ON true")
      assert String.contains?(sql, "LEFT JOIN LATERAL GENERATE_SERIES(?, ?) AS numbers ON true")
      assert params == [1, 5]
    end
    
    test "works with other Selecto features", %{domain: domain} do
      selecto = Selecto.configure(domain, [], validate: false)
        |> Selecto.select(["film.title", "features.value"])
        |> Selecto.filter([{"film.rating", "PG"}])
        |> Selecto.lateral_join(
          :inner,
          {:unnest, "film.special_features"},
          "features"
        )
        |> Selecto.order_by([{"film.title", :asc}])
      
      {sql, params} = Selecto.to_sql(selecto)
      
      # Should contain all query elements
      assert String.contains?(sql, "SELECT")
      assert String.contains?(sql, "WHERE")
      assert String.contains?(sql, "film.rating")
      assert String.contains?(sql, "INNER JOIN LATERAL")
      assert String.contains?(sql, "ORDER BY")
      assert String.contains?(sql, "film.title")
      
      # Should have parameter for the rating filter
      assert "PG" in params
    end
  end
  
  describe "LATERAL join validation errors" do
    setup do
      domain = %{
        name: "customer_domain", 
        source: %{
          source_table: "customer",
          primary_key: :customer_id,
          fields: [:customer_id, :first_name, :last_name, :email],
          redact_fields: [],
          columns: %{
            customer_id: %{type: :integer},
            first_name: %{type: :string},
            last_name: %{type: :string},
            email: %{type: :string}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      {:ok, domain: domain}
    end
    
    test "raises error for invalid correlation reference", %{domain: domain} do
      rental_domain = %{
        source: %{source_table: "rental"},
        schemas: %{},
        joins: %{}
      }
      
      assert_raise Selecto.Advanced.LateralJoin.CorrelationError, ~r/Cannot reference field/, fn ->
        Selecto.configure(domain, [], validate: false)
        |> Selecto.lateral_join(
          :left,
          fn _base ->
            Selecto.configure(rental_domain, [], validate: false)
            |> Selecto.select([{"count", :count}])
            |> Selecto.filter([{"customer_id", {:ref, "customer.invalid_field"}}])
          end,
          "invalid_lateral"
        )
      end
    end
    
    test "provides helpful error message with available fields", %{domain: domain} do
      rental_domain = %{
        source: %{source_table: "rental"},
        schemas: %{},
        joins: %{}
      }
      
      try do
        Selecto.configure(domain, [], validate: false)
        |> Selecto.lateral_join(
          :left,
          fn _base ->
            Selecto.configure(rental_domain, [], validate: false)
            |> Selecto.select([{"count", :count}])
            |> Selecto.filter([{"customer_id", {:ref, "customer.nonexistent"}}])
          end,
          "error_lateral"
        )
        
        flunk("Expected CorrelationError to be raised")
      rescue
        error in Selecto.Advanced.LateralJoin.CorrelationError ->
          assert error.referenced_field == "customer.nonexistent"
          assert "customer.customer_id" in error.available_fields
          assert "customer.first_name" in error.available_fields
      end
    end
    
    test "validates table function correlation references", %{domain: domain} do
      assert_raise Selecto.Advanced.LateralJoin.CorrelationError, fn ->
        Selecto.configure(domain, [], validate: false)
        |> Selecto.lateral_join(
          :inner,
          {:unnest, "customer.nonexistent_array"},
          "bad_unnest"
        )
      end
    end
  end
  
  describe "LATERAL join edge cases" do
    setup do
      domain = %{
        name: "test_domain",
        source: %{
          source_table: "test_table",
          primary_key: :id,
          fields: [:id, :name, :data],
          redact_fields: [],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            data: %{type: :map}
          },
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      
      {:ok, domain: domain}
    end
    
    test "handles complex function arguments", %{domain: domain} do
      selecto = Selecto.configure(domain, [], validate: false)
        |> Selecto.lateral_join(
          :inner,
          {:function, :complex_func, [
            {:ref, "test_table.id"},
            "string_literal", 
            42,
            3.14,
            true,
            {:literal, "explicit_literal"}
          ]},
          "complex_result"
        )
      
      {sql, params} = Selecto.to_sql(selecto)
      
      assert String.contains?(sql, "COMPLEX_FUNC(test_table.id, ?, ?, ?, ?, ?)")
      assert params == ["string_literal", 42, 3.14, true, "explicit_literal"]
    end
    
    test "works with existing joins in base query", %{domain: domain} do
      # Add a join to the domain
      domain_with_join = put_in(domain.joins, %{
        related: %{
          target_table: "related",
          fields: ["related_id", "test_id", "value"]
        }
      })
      
      base_selecto = %Selecto{
        domain: domain_with_join,
        postgrex_opts: [],
        set: %{
          joins: %{
            related: %{
              target_table: "related", 
              fields: ["related_id", "value"]
            }
          }
        }
      }
      
      selecto = base_selecto
        |> Selecto.lateral_join(
          :left,
          {:function, :test_func, [
            {:ref, "test_table.id"},
            {:ref, "related.related_id"}
          ]},
          "lateral_result"
        )
      
      lateral_joins = Map.get(selecto.set, :lateral_joins, [])
      assert length(lateral_joins) == 1
      
      [lateral_spec] = lateral_joins
      assert lateral_spec.validated == true
    end
  end
end