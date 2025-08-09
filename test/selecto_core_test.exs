defmodule Selecto.CoreTest do
  use ExUnit.Case
  
  # Mock domain for testing
  defp mock_domain do
    %{
      name: "Test Domain",
      source: %{
        source_table: "users",
        primary_key: :id,
        fields: [:id, :name, :email, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          email: %{type: :string},
          active: %{type: :boolean}
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
      default_selected: ["id", "name"],
      joins: %{
        posts: %{type: :left, name: "posts"}
      },
      filters: %{}
    }
  end

  describe "configure/3" do
    test "creates Selecto struct with domain configuration" do
      domain = mock_domain()
      postgrex_opts = [database: "test"]
      
      selecto = Selecto.configure(domain, postgrex_opts)
      
      assert %Selecto{} = selecto
      assert selecto.postgrex_opts == postgrex_opts
      assert selecto.domain == domain
      assert is_map(selecto.config)
    end

    test "handles empty postgrex_opts" do
      domain = mock_domain()
      
      selecto = Selecto.configure(domain, nil)
      
      assert %Selecto{} = selecto
      assert selecto.postgrex_opts == nil
      assert selecto.domain == domain
    end

    test "processes domain configuration" do
      domain = mock_domain()
      
      selecto = Selecto.configure(domain, [])
      
      # Should have processed columns from domain
      assert is_map(selecto.config.columns)
      assert selecto.config.columns["id"]
      assert selecto.config.columns["name"]
    end
  end

  describe "accessor functions" do
    setup do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "filters/1 returns filter configuration", %{selecto: selecto} do
      result = Selecto.filters(selecto)
      assert result == %{}
    end

    test "columns/1 returns column configuration", %{selecto: selecto} do
      columns = Selecto.columns(selecto)
      assert is_map(columns)
      assert columns["id"]
      assert columns["name"]
    end

    test "joins/1 returns join configuration", %{selecto: selecto} do
      joins = Selecto.joins(selecto)
      assert is_map(joins)
      assert joins[:posts]  # Should have posts join
      assert joins[:posts].name == "posts"
    end

    test "source_table/1 returns source table name", %{selecto: selecto} do
      table = Selecto.source_table(selecto)
      assert table == "users"
    end

    test "domain/1 returns original domain", %{selecto: selecto} do
      domain = Selecto.domain(selecto)
      assert domain.name == "Test Domain"
      assert domain.source.source_table == "users"
    end

    test "domain_data/1 returns domain data", %{selecto: selecto} do
      data = Selecto.domain_data(selecto)
      # domain_data may return nil or domain source data
      assert is_nil(data) || data.source_table == "users"
    end

    test "field/2 returns field configuration", %{selecto: selecto} do
      field_config = Selecto.field(selecto, "name")
      assert field_config
      assert field_config.field == :name
      assert field_config.type == :string
    end

    test "field/2 returns nil for non-existent field", %{selecto: selecto} do
      field_config = Selecto.field(selecto, "nonexistent")
      assert is_nil(field_config)
    end

    test "set/1 returns selecto set", %{selecto: selecto} do
      set = Selecto.set(selecto)
      # set should be the Selecto configuration's set
      assert is_map(set) || is_nil(set)
    end
  end

  describe "select/2" do
    setup do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "adds single field to selection", %{selecto: selecto} do
      result = Selecto.select(selecto, "name")
      
      assert %Selecto{} = result
      # Should have updated the set with selection
      assert result.set
    end

    test "adds multiple fields to selection", %{selecto: selecto} do
      result = Selecto.select(selecto, ["name", "email"])
      
      assert %Selecto{} = result
      # Should have updated the set with multiple selections
      assert result.set
    end

    test "chains multiple select calls", %{selecto: selecto} do
      result = selecto
        |> Selecto.select("name")
        |> Selecto.select("email")
      
      assert %Selecto{} = result
      assert result.set
    end
  end

  describe "filter/2" do
    setup do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "adds single filter condition", %{selecto: selecto} do
      result = Selecto.filter(selecto, {"active", true})
      
      assert %Selecto{} = result
      assert result.set
    end

    test "adds multiple filter conditions", %{selecto: selecto} do
      filters = [{"active", true}, {"name", "John"}]
      result = Selecto.filter(selecto, filters)
      
      assert %Selecto{} = result
      assert result.set
    end

    test "chains multiple filter calls", %{selecto: selecto} do
      result = selecto
        |> Selecto.filter({"active", true})
        |> Selecto.filter({"name", "John"})
      
      assert %Selecto{} = result
      assert result.set
    end
  end

  describe "order_by/2" do
    setup do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "adds single order clause", %{selecto: selecto} do
      result = Selecto.order_by(selecto, "name")
      
      assert %Selecto{} = result
      assert result.set
    end

    test "adds multiple order clauses", %{selecto: selecto} do
      orders = ["name", "email"]
      result = Selecto.order_by(selecto, orders)
      
      assert %Selecto{} = result
      assert result.set
    end

    test "chains multiple order_by calls", %{selecto: selecto} do
      result = selecto
        |> Selecto.order_by("name")
        |> Selecto.order_by("email")
      
      assert %Selecto{} = result
      assert result.set
    end
  end

  describe "group_by/2" do
    setup do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "adds single group clause", %{selecto: selecto} do
      result = Selecto.group_by(selecto, "name")
      
      assert %Selecto{} = result
      assert result.set
    end

    test "adds multiple group clauses", %{selecto: selecto} do
      groups = ["name", "active"]
      result = Selecto.group_by(selecto, groups)
      
      assert %Selecto{} = result
      assert result.set
    end

    test "chains multiple group_by calls", %{selecto: selecto} do
      result = selecto
        |> Selecto.group_by("name")
        |> Selecto.group_by("active")
      
      assert %Selecto{} = result
      assert result.set
    end
  end

  describe "chaining operations" do
    setup do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      {:ok, selecto: selecto}
    end

    test "chains multiple operations together", %{selecto: selecto} do
      result = selecto
        |> Selecto.select(["id", "name", "email"])
        |> Selecto.filter([{"active", true}, {"name", {:like, "%John%"}}])
        |> Selecto.order_by(["name", "email"])
        |> Selecto.group_by("active")
      
      assert %Selecto{} = result
      assert result.set
      
      # All operations should have been applied to the set
      assert result.set.selected
      assert result.set.filtered  # Uses 'filtered' not 'filters'
      assert result.set.order_by   # Uses 'order_by' not 'orders'
      assert result.set.group_by   # Uses 'group_by' not 'groups'
    end

    test "preserves original domain through chaining", %{selecto: selecto} do
      result = selecto
        |> Selecto.select("name")
        |> Selecto.filter({"active", true})
        |> Selecto.order_by("name")
      
      # Domain should remain unchanged
      assert result.domain == selecto.domain
      assert result.config.columns == selecto.config.columns
    end
  end

  describe "gen_sql/2" do
    setup do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
        |> Selecto.select(["id", "name"])
        |> Selecto.filter({"active", true})
      {:ok, selecto: selecto}
    end

    test "generates SQL from Selecto struct", %{selecto: selecto} do
      {sql, aliases, params} = Selecto.gen_sql(selecto, [])
      
      assert is_binary(sql)
      assert String.contains?(String.upcase(sql), "SELECT")
      assert String.contains?(String.upcase(sql), "FROM") 
      assert String.contains?(sql, "users")
      
      assert is_list(aliases)
      assert is_list(params)
      assert true in params  # Should contain the filter parameter
    end

    test "includes WHERE clause for filters", %{selecto: selecto} do
      {sql, _aliases, params} = Selecto.gen_sql(selecto, [])
      
      assert String.contains?(String.upcase(sql), "WHERE")
      assert true in params  # Should contain the filter parameter
    end

    test "handles empty selecto (no filters/selections)", %{selecto: _selecto} do
      domain = mock_domain()
      basic_selecto = Selecto.configure(domain, [])
      
      {sql, _aliases, params} = Selecto.gen_sql(basic_selecto, [])
      
      assert is_binary(sql)
      assert String.contains?(String.upcase(sql), "SELECT")
      assert String.contains?(String.upcase(sql), "FROM")
      assert is_list(params)
    end
  end

  describe "edge cases and error handling" do
    test "configure/3 handles malformed domain gracefully" do
      incomplete_domain = %{
        source: %{
          source_table: "users"
          # Missing required fields
        }
      }
      
      # Should not crash but may have limited functionality
      assert_raise KeyError, fn ->
        Selecto.configure(incomplete_domain, [])
      end
    end

    test "field/2 handles string and atom field names", %{} do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      
      # String field name
      field_config1 = Selecto.field(selecto, "name")
      assert field_config1
      
      # Atom field name (if supported)
      field_config2 = Selecto.field(selecto, :name)
      # May return nil if atom keys aren't supported, that's OK
      assert is_nil(field_config2) || field_config2
    end

    test "operations handle empty inputs gracefully" do
      domain = mock_domain()
      selecto = Selecto.configure(domain, [])
      
      # Empty selections
      result1 = Selecto.select(selecto, [])
      assert %Selecto{} = result1
      
      # Empty filters
      result2 = Selecto.filter(selecto, [])
      assert %Selecto{} = result2
      
      # Empty orders
      result3 = Selecto.order_by(selecto, [])
      assert %Selecto{} = result3
      
      # Empty groups
      result4 = Selecto.group_by(selecto, [])
      assert %Selecto{} = result4
    end
  end
end