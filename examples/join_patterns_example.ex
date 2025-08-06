defmodule Selecto.Examples.JoinPatternsExample do
  @moduledoc """
  Example configurations showing how to use the new simplified join patterns
  in Selecto. These examples demonstrate the various join types and their configurations.
  """

  # Example domain configuration showing different join patterns
  def example_domain do
    %{
      schemas: %{
        # Main entities
        SelectoExample.Store.Order => order_schema(),
        SelectoExample.Store.Product => product_schema(),
        SelectoExample.Store.Customer => customer_schema(),
        SelectoExample.Store.Category => category_schema(),
        SelectoExample.Store.Tag => tag_schema(),
        SelectoExample.Store.Employee => employee_schema(),
        SelectoExample.Store.Department => department_schema()
      },
      # Different join pattern examples
      joins: %{
        # Many-to-many tagging example
        products: %{
          type: :tagging,
          tag_field: :name,
          name: "Product Tags"
        },
        
        # Star schema dimension example (typical OLAP setup)
        customer: %{
          type: :star_dimension,
          display_field: :full_name,
          name: "Customer Dimension"
        },
        
        # Snowflake schema dimension example
        category: %{
          type: :snowflake_dimension,
          display_field: :name,
          normalization_joins: [
            %{table: "category_groups", alias: "cg", local_key: :group_id, remote_key: :id}
          ],
          name: "Category Hierarchy"
        },
        
        # Hierarchical adjacency list example (organizational chart)
        manager: %{
          type: :hierarchical,
          hierarchy_type: :adjacency_list,
          depth_limit: 5,
          name: "Management Chain"
        },
        
        # Hierarchical materialized path example (category tree)
        parent_category: %{
          type: :hierarchical,
          hierarchy_type: :materialized_path,
          path_field: :path,
          path_separator: "/",
          name: "Category Path"
        },
        
        # Hierarchical closure table example (complex hierarchies)
        department_hierarchy: %{
          type: :hierarchical,
          hierarchy_type: :closure_table,
          closure_table: "department_closure",
          ancestor_field: :ancestor_id,
          descendant_field: :descendant_id,
          depth_field: :depth,
          name: "Department Structure"
        },
        
        # Traditional dimension table example (existing functionality)
        region: %{
          type: :dimension,
          dimension: :name,
          name: "Sales Region"
        }
      }
    }
  end

  # Schema configurations
  defp order_schema do
    %{
      source_table: "orders",
      fields: [:id, :total, :order_date, :customer_id, :region_id],
      redact_fields: [],
      associations: %{
        customer: %{
          field: :customer,
          owner_key: :customer_id,
          related_key: :id,
          queryable: SelectoExample.Store.Customer
        },
        products: %{
          field: :products,
          owner_key: :id,
          related_key: :order_id,  # through order_items
          queryable: SelectoExample.Store.Product
        },
        region: %{
          field: :region,
          owner_key: :region_id,
          related_key: :id,
          queryable: SelectoExample.Store.Region
        }
      }
    }
  end

  defp product_schema do
    %{
      source_table: "products",
      fields: [:id, :name, :price, :category_id],
      redact_fields: [],
      associations: %{
        category: %{
          field: :category,
          owner_key: :category_id,
          related_key: :id,
          queryable: SelectoExample.Store.Category
        },
        tags: %{
          field: :tags,
          owner_key: :id,
          related_key: :product_id,  # through product_tags
          queryable: SelectoExample.Store.Tag
        }
      }
    }
  end

  defp customer_schema do
    %{
      source_table: "customers",
      fields: [:id, :first_name, :last_name, :full_name, :email],
      redact_fields: [:email],
      associations: %{}
    }
  end

  defp category_schema do
    %{
      source_table: "categories",
      fields: [:id, :name, :parent_id, :path, :group_id],
      redact_fields: [],
      associations: %{
        parent_category: %{
          field: :parent,
          owner_key: :parent_id,
          related_key: :id,
          queryable: SelectoExample.Store.Category
        }
      }
    }
  end

  defp tag_schema do
    %{
      source_table: "tags",
      fields: [:id, :name, :color],
      redact_fields: [],
      associations: %{}
    }
  end

  defp employee_schema do
    %{
      source_table: "employees",
      fields: [:id, :name, :manager_id, :department_id],
      redact_fields: [],
      associations: %{
        manager: %{
          field: :manager,
          owner_key: :manager_id,
          related_key: :id,
          queryable: SelectoExample.Store.Employee
        },
        department: %{
          field: :department,
          owner_key: :department_id,
          related_key: :id,
          queryable: SelectoExample.Store.Department
        }
      }
    }
  end

  defp department_schema do
    %{
      source_table: "departments",
      fields: [:id, :name, :parent_id],
      redact_fields: [],
      associations: %{
        department_hierarchy: %{
          field: :hierarchy,
          owner_key: :id,
          related_key: :descendant_id,  # through closure table
          queryable: SelectoExample.Store.Department
        }
      }
    }
  end

  @doc """
  Example usage showing how the join configuration would be used:
  
  ## Tagging Example
  ```elixir
  # Products with their tags aggregated
  domain = example_domain()
  selecto = Selecto.new(SelectoExample.Store.Product, domain)
    |> Selecto.select([:name, :price, "tags_list"])  # Uses the custom column from tagging config
    |> Selecto.filter([{"tags_filter", ["electronics", "gadgets"]}])  # Uses the custom filter
  ```
  
  ## Hierarchical Example
  ```elixir
  # Employees with their management chain
  selecto = Selecto.new(SelectoExample.Store.Employee, domain)
    |> Selecto.select([:name, "manager_path", "manager_level"])  # Uses hierarchical custom columns
    |> Selecto.filter([{"manager_level", {"<=", 3}}])  # Show only up to 3 levels deep
  ```
  
  ## Star Schema Example
  ```elixir
  # Orders with customer dimension for OLAP analysis
  selecto = Selecto.new(SelectoExample.Store.Order, domain)
    |> Selecto.select([:total, :order_date, "customer_display"])  # Uses dimension display field
    |> Selecto.group_by(["customer_display"])
    |> Selecto.filter([{"customer_facet", ["premium_customers"]}])  # Uses faceted filter
  ```
  """
  def usage_examples do
    :ok
  end
end