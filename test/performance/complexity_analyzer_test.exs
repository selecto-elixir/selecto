defmodule Selecto.Performance.ComplexityAnalyzerTest do
  use ExUnit.Case, async: true
  alias Selecto.Performance.ComplexityAnalyzer

  describe "analyze/2" do
    test "returns ok for simple query with no joins" do
      selecto = build_simple_selecto()

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert analysis.score < 50
      assert Enum.empty?(analysis.blocking_issues)
    end

    test "warns on queries with many joins" do
      selecto = build_selecto_with_joins(7)

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert analysis.score >= 70
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "joins"))
    end

    test "blocks queries with too many joins" do
      selecto = build_selecto_with_joins(12)

      assert {:error, :too_complex, analysis} = ComplexityAnalyzer.analyze(selecto, max_joins: 10)
      assert Enum.any?(analysis.blocking_issues, &String.contains?(&1, "12 joins"))
    end

    test "respects custom max_complexity option" do
      selecto = build_selecto_with_joins(5)

      # With low threshold, should block
      assert {:error, :too_complex, _analysis} =
               ComplexityAnalyzer.analyze(selecto, max_complexity: 30)

      # With high threshold, should pass
      assert {:ok, _analysis} = ComplexityAnalyzer.analyze(selecto, max_complexity: 100)
    end

    test "warns on missing filters" do
      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          # No filters
          filtered: [],
          order_by: [],
          group_by: []
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "full table scan"))
    end

    test "detects subselects and adds complexity" do
      selecto = build_simple_selecto()

      selecto = %{
        selecto
        | set:
            Map.put(selecto.set, :subselected, [
              %{fields: ["id"], format: :json_agg},
              %{fields: ["name"], format: :array_agg}
            ])
      }

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert analysis.details.subselect_count == 2
      # 15 points per subselect
      assert analysis.score >= 30
    end

    test "warns on large IN clauses" do
      large_list = Enum.to_list(1..150)

      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          filtered: [{"id", {:in, large_list}}],
          order_by: [],
          group_by: []
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "IN clause"))
      assert analysis.details.max_in_clause_size == 150
    end

    test "accepts IN clauses under threshold" do
      small_list = Enum.to_list(1..50)

      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          filtered: [{"id", {:in, small_list}}],
          order_by: [],
          group_by: []
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert analysis.details.max_in_clause_size == 50
      # Should not have IN clause warning since it's under threshold
      refute Enum.any?(analysis.warnings, &String.contains?(&1, "IN clause"))
    end

    test "detects LIKE patterns with leading wildcards" do
      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          filtered: [
            {"name", {:like, "%smith"}},
            {"email", {:like, "%@example.com"}}
          ],
          order_by: [],
          group_by: []
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "leading wildcard"))
      assert analysis.details.leading_wildcard_count == 2
    end

    test "accepts LIKE patterns without leading wildcards" do
      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          filtered: [{"name", {:like, "smith%"}}],
          order_by: [],
          group_by: []
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert analysis.details.leading_wildcard_count == 0
    end

    test "detects post-retarget filters" do
      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          filtered: [{"status", "active"}],
          post_retarget_filters: [
            {"aggregated_total", {:gt, 1000}}
          ],
          order_by: [],
          group_by: []
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "post-retarget"))
      assert analysis.details.post_retarget_filter_count == 1
    end

    test "warns on high GROUP BY count" do
      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: [
            {:func, "SUM", ["amount"]},
            "category",
            "subcategory",
            "region",
            "country",
            "city",
            "year"
          ],
          filtered: [],
          order_by: [],
          group_by: ["category", "subcategory", "region", "country", "city", "year"]
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "GROUP BY"))
      assert analysis.details.group_by_count == 6
    end

    test "handles map-style filters (from form submissions)" do
      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          filtered: [
            %{"filter" => "status", "comp" => "IN", "value" => Enum.to_list(1..150)},
            %{"filter" => "name", "comp" => "LIKE", "value" => "%smith"}
          ],
          order_by: [],
          group_by: []
        })

      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      # Should detect large IN clause
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "IN clause"))
      # Should detect leading wildcard LIKE
      assert Enum.any?(analysis.warnings, &String.contains?(&1, "leading wildcard"))
    end

    test "combines multiple complexity factors" do
      large_list = Enum.to_list(1..150)

      selecto = build_selecto_with_joins(7)

      selecto = %{
        selecto
        | set:
            selecto.set
            |> Map.put(:subselected, [%{fields: ["id"], format: :json_agg}])
            |> Map.put(:filtered, [
              {"id", {:in, large_list}},
              {"name", {:like, "%smith"}}
            ])
            |> Map.put(:post_retarget_filters, [{"total", {:gt, 100}}])
            |> Map.put(:group_by, ["cat1", "cat2", "cat3", "cat4", "cat5", "cat6"])
      }

      # Should be blocked due to high complexity score
      assert {:error, :too_complex, analysis} = ComplexityAnalyzer.analyze(selecto)
      # Should have high score from multiple factors
      assert analysis.score > 100
      # Should have multiple warnings
      assert length(analysis.warnings) >= 5
    end

    test "block_on_warnings option treats warnings as blocking" do
      selecto =
        build_simple_selecto()
        |> Map.put(:set, %{
          selected: ["id", "name"],
          # Leading wildcard warning
          filtered: [{"name", {:like, "%smith"}}],
          order_by: [],
          group_by: []
        })

      # Without block_on_warnings, should pass with warning
      assert {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      assert length(analysis.warnings) > 0

      # With block_on_warnings, should be blocked
      assert {:error, :too_complex, _analysis} =
               ComplexityAnalyzer.analyze(selecto, block_on_warnings: true)
    end
  end

  describe "format_summary/1" do
    test "formats analysis summary with all sections" do
      selecto = build_selecto_with_joins(7)
      selecto = %{selecto | set: %{selecto.set | filtered: [{"name", {:like, "%smith"}}]}}

      {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      summary = ComplexityAnalyzer.format_summary(analysis)

      assert summary =~ "Query Complexity Analysis"
      assert summary =~ "Score:"
      assert summary =~ "Warnings:"
      assert summary =~ "Recommendations:"
    end

    test "formats blocked query summary" do
      selecto = build_selecto_with_joins(15)

      {:error, :too_complex, analysis} =
        ComplexityAnalyzer.analyze(selecto, max_joins: 10)

      summary = ComplexityAnalyzer.format_summary(analysis)

      assert summary =~ "Blocking Issues:"
      assert summary =~ "15 joins"
    end
  end

  describe "safe_to_execute?/2" do
    test "returns true for safe queries" do
      selecto = build_simple_selecto()
      assert ComplexityAnalyzer.safe_to_execute?(selecto)
    end

    test "returns false for complex queries" do
      selecto = build_selecto_with_joins(15)
      refute ComplexityAnalyzer.safe_to_execute?(selecto, max_joins: 10)
    end
  end

  describe "nested joins" do
    test "counts nested joins recursively" do
      selecto = build_selecto_with_nested_joins()

      {:ok, analysis} = ComplexityAnalyzer.analyze(selecto)
      # Should count: posts (1) + tags (1) + categories (1) = 3
      assert analysis.details.join_count == 3
    end
  end

  # Helper functions

  defp build_simple_selecto do
    %Selecto{
      postgrex_opts: :mock_repo,
      adapter: nil,
      connection: nil,
      domain: %{
        name: "Test",
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
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      },
      config: %{
        source: %{source_table: "users"},
        source_table: "users",
        primary_key: :id,
        columns: %{},
        joins: %{},
        filters: %{},
        domain_data: nil
      },
      set: %{
        selected: ["id", "name"],
        filtered: [{"status", "active"}],
        order_by: [],
        group_by: []
      }
    }
  end

  defp build_selecto_with_joins(count) do
    joins =
      for i <- 1..count, into: %{} do
        join_name = :"join_#{i}"

        {join_name,
         %{
           type: :left,
           name: "table_#{i}",
           source: "table_#{i}"
         }}
      end

    selecto = build_simple_selecto()

    %{
      selecto
      | config: %{selecto.config | joins: joins},
        set: Map.put(selecto.set, :active_joins, Map.keys(joins))
    }
  end

  defp build_selecto_with_nested_joins do
    joins = %{
      posts: %{
        type: :left,
        name: "posts",
        source: "posts",
        joins: %{
          tags: %{
            type: :left,
            name: "tags",
            source: "tags"
          },
          categories: %{
            type: :left,
            name: "categories",
            source: "categories"
          }
        }
      }
    }

    selecto = build_simple_selecto()
    %{selecto | config: %{selecto.config | joins: joins}}
  end
end
