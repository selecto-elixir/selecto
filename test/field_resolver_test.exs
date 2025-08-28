defmodule Selecto.FieldResolverTest do
  use ExUnit.Case, async: true
  
  alias Selecto.FieldResolver
  alias Selecto.Error
  
  setup do
    # Mock Selecto structure with source and joins
    selecto = %Selecto{
      config: %{
        source: %{
          fields: [:id, :name, :email, :created_at],
          redact_fields: [:email],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            email: %{type: :string},
            created_at: %{type: :utc_datetime}
          }
        },
        joins: %{
          posts: %{
            fields: %{
              "posts[id]" => %{type: :integer, alias: nil},
              "posts[title]" => %{type: :string, alias: nil},
              "posts[content]" => %{type: :text, alias: nil}
            }
          },
          comments: %{
            fields: %{
              "comments[id]" => %{type: :integer, alias: nil},
              "comments[content]" => %{type: :text, alias: nil},
              "comments[created_at]" => %{type: :utc_datetime, alias: nil}
            }
          },
          profile: %{
            fields: %{
              "profile[id]" => %{type: :integer, alias: nil},
              "profile[bio]" => %{type: :text, alias: nil},
              "profile[avatar_url]" => %{type: :string, alias: nil}
            }
          }
        }
      }
    }
    
    {:ok, selecto: selecto}
  end
  
  describe "resolve_field/2" do
    test "resolves simple field from source table", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "name")
      
      assert field_info.name == "name"
      assert field_info.qualified_name == "name"
      assert field_info.source_join == :selecto_root
      assert field_info.type == :string
      assert field_info.table_alias == "selecto_root"
    end
    
    test "resolves qualified field from join", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "posts.title")
      
      assert field_info.name == "title"
      assert field_info.qualified_name == "posts.title"
      assert field_info.source_join == :posts
      assert field_info.type == :string
      assert field_info.table_alias == "posts"
    end
    
    test "resolves aliased field reference", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, {:field, "name", alias: "user_name"})
      
      assert field_info.name == "name"
      assert field_info.qualified_name == "name"
      assert field_info.alias == "user_name"
      assert field_info.source_join == :selecto_root
    end
    
    test "resolves disambiguated field reference", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, {:disambiguated_field, "id", from: "posts"})
      
      assert field_info.name == "id"
      assert field_info.qualified_name == "posts.id"
      assert field_info.source_join == :posts
      assert field_info.type == :integer
    end
    
    test "resolves explicitly qualified field reference", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, {:qualified_field, "comments.content"})
      
      assert field_info.name == "content"
      assert field_info.qualified_name == "comments.content"
      assert field_info.source_join == :comments
      assert field_info.type == :text
    end
    
    test "handles atom field references", %{selecto: selecto} do
      {:ok, field_info} = FieldResolver.resolve_field(selecto, :name)
      
      assert field_info.name == "name"
      assert field_info.qualified_name == "name"
      assert field_info.source_join == :selecto_root
    end
    
    test "returns error for non-existent field", %{selecto: selecto} do
      {:error, error} = FieldResolver.resolve_field(selecto, "non_existent")
      
      assert %Error{type: :field_resolution_error} = error
      assert error.message =~ "Field 'non_existent' not found"
      assert is_list(error.details.suggestions)
      assert is_list(error.details.available_fields)
    end
    
    test "returns error for ambiguous field with suggestions", %{selecto: selecto} do
      # The 'id' field already exists in source and all joins, creating natural ambiguity
      # Since source table has 'id' and joins like posts, comments, profile also have 'id'
      # But our current logic prioritizes source fields, so let's create a proper ambiguity test
      
      # Remove 'id' from source to force ambiguity between joins
      selecto = put_in(selecto.config.source.fields, [:name, :email, :created_at])
      selecto = put_in(selecto.config.source.columns, Map.delete(selecto.config.source.columns, :id))
      
      {:error, error} = FieldResolver.resolve_field(selecto, "id")
      
      assert %Error{type: :field_resolution_error} = error
      assert error.message =~ "Ambiguous field reference"
      assert is_list(error.details.available_options)
    end
    
    test "returns error for non-existent join", %{selecto: selecto} do
      {:error, error} = FieldResolver.resolve_field(selecto, "non_existent_join.field")
      
      assert %Error{type: :field_resolution_error} = error
      assert error.message =~ "Join 'non_existent_join' not found"
      assert is_list(error.details.available_joins)
    end
    
    test "returns error for non-existent field in existing join", %{selecto: selecto} do
      {:error, error} = FieldResolver.resolve_field(selecto, "posts.non_existent_field")
      
      assert %Error{type: :field_resolution_error} = error
      assert error.message =~ "Field 'non_existent_field' not found in join 'posts'"
      assert is_list(error.details.available_fields_in_join)
    end
    
    test "returns error for invalid field reference format", %{selecto: selecto} do
      {:error, error} = FieldResolver.resolve_field(selecto, {:invalid_format, "field"})
      
      assert %Error{type: :field_resolution_error} = error
      assert error.message =~ "Invalid field reference format"
    end
  end
  
  describe "get_available_fields/1" do
    test "returns all available fields from source and joins", %{selecto: selecto} do
      fields = FieldResolver.get_available_fields(selecto)
      
      # Source fields (excluding redacted email)
      assert Map.has_key?(fields, "id")
      assert Map.has_key?(fields, "name")
      assert Map.has_key?(fields, "created_at")
      refute Map.has_key?(fields, "email")  # redacted
      
      # Join fields
      assert Map.has_key?(fields, "posts.id")
      assert Map.has_key?(fields, "posts.title")
      assert Map.has_key?(fields, "comments.content")
      assert Map.has_key?(fields, "profile.bio")
      
      # Check field info structure
      assert fields["name"].source_join == :selecto_root
      assert fields["posts.title"].source_join == :posts
      assert fields["posts.title"].type == :string
    end
  end
  
  describe "suggest_fields/2" do
    test "returns field suggestions based on partial match", %{selecto: selecto} do
      suggestions = FieldResolver.suggest_fields(selecto, "con")
      
      assert "posts.content" in suggestions
      assert "comments.content" in suggestions
      assert length(suggestions) <= 5
    end
    
    test "returns suggestions sorted by relevance", %{selecto: selecto} do
      suggestions = FieldResolver.suggest_fields(selecto, "id")
      
      # Should prioritize exact or closer matches
      assert "id" in suggestions
      assert "posts.id" in suggestions
      assert "comments.id" in suggestions
      assert "profile.id" in suggestions
    end
    
    test "returns empty list for no matches", %{selecto: selecto} do
      suggestions = FieldResolver.suggest_fields(selecto, "xyzzyx")
      
      assert suggestions == []
    end
  end
  
  describe "is_ambiguous_field?/2" do
    test "returns false for unambiguous field", %{selecto: selecto} do
      refute FieldResolver.is_ambiguous_field?(selecto, "name")
      refute FieldResolver.is_ambiguous_field?(selecto, "title")
    end
    
    test "returns true for ambiguous field", %{selecto: selecto} do
      # Add another join with 'id' to create ambiguity  
      selecto = put_in(selecto.config.joins[:tags], %{
        fields: %{"tags[id]" => %{type: :integer, alias: nil}}
      })
      
      assert FieldResolver.is_ambiguous_field?(selecto, "id")
    end
  end
  
  describe "get_disambiguation_options/2" do
    test "returns all disambiguation options for ambiguous field", %{selecto: selecto} do
      # Add joins to create ambiguity for 'id'
      selecto = put_in(selecto.config.joins[:tags], %{
        fields: %{"tags[id]" => %{type: :integer, alias: nil}}
      })
      
      options = FieldResolver.get_disambiguation_options(selecto, "id")
      
      assert length(options) >= 2
      
      # Check that options contain field info
      option_qualified_names = Enum.map(options, & &1.qualified_name)
      assert "id" in option_qualified_names
      assert "posts.id" in option_qualified_names
      assert "tags.id" in option_qualified_names
    end
    
    test "returns single option for unambiguous field", %{selecto: selecto} do
      options = FieldResolver.get_disambiguation_options(selecto, "name")
      
      assert length(options) == 1
      assert hd(options).qualified_name == "name"
    end
  end
  
  describe "validate_field_references/2" do
    test "returns :ok for valid field references", %{selecto: selecto} do
      field_refs = ["name", "posts.title", "comments.content"]
      
      assert FieldResolver.validate_field_references(selecto, field_refs) == :ok
    end
    
    test "returns error list for invalid field references", %{selecto: selecto} do
      field_refs = ["name", "invalid_field", "posts.invalid_field"]
      
      {:error, errors} = FieldResolver.validate_field_references(selecto, field_refs)
      
      assert length(errors) == 2
      assert Enum.all?(errors, &match?(%Error{type: :field_resolution_error}, &1))
    end
    
    test "handles mixed valid and invalid references", %{selecto: selecto} do
      field_refs = ["name", "invalid", "posts.title", "posts.invalid"]
      
      {:error, errors} = FieldResolver.validate_field_references(selecto, field_refs)
      
      assert length(errors) == 2
    end
  end
  
  describe "field extraction" do
    test "extracts field names from bracket notation" do
      # Test the private extract_field_name function through public interface
      selecto = %Selecto{
        config: %{
          source: %{fields: [], redact_fields: [], columns: %{}},
          joins: %{
            test_join: %{
              fields: %{
                "test_join[complex_field_name]" => %{type: :string, alias: nil}
              }
            }
          }
        }
      }
      
      fields = FieldResolver.get_available_fields(selecto)
      
      assert Map.has_key?(fields, "test_join.complex_field_name")
    end
    
    test "handles various field key formats" do
      selecto = %Selecto{
        config: %{
          source: %{fields: [], redact_fields: [], columns: %{}},
          joins: %{
            test_join: %{
              fields: %{
                "simple_field" => %{type: :string, alias: nil},
                "test_join[bracketed_field]" => %{type: :integer, alias: nil},
                :atom_field => %{type: :boolean, alias: nil}
              }
            }
          }
        }
      }
      
      fields = FieldResolver.get_available_fields(selecto)
      
      assert Map.has_key?(fields, "test_join.simple_field")
      assert Map.has_key?(fields, "test_join.bracketed_field")
      assert Map.has_key?(fields, "test_join.atom_field")
    end
  end
  
  describe "edge cases" do
    test "handles empty selecto configuration gracefully" do
      empty_selecto = %Selecto{
        config: %{
          source: %{fields: [], redact_fields: [], columns: %{}},
          joins: %{}
        }
      }
      
      fields = FieldResolver.get_available_fields(empty_selecto)
      suggestions = FieldResolver.suggest_fields(empty_selecto, "test")
      
      assert fields == %{}
      assert suggestions == []
    end
    
    test "handles field references with special characters" do
      selecto = %Selecto{
        config: %{
          source: %{
            fields: [:"special-field", :"field_with_numbers123"],
            redact_fields: [],
            columns: %{
              "special-field": %{type: :string},
              "field_with_numbers123": %{type: :integer}
            }
          },
          joins: %{}
        }
      }
      
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "special-field")
      assert field_info.name == "special-field"
      
      {:ok, field_info} = FieldResolver.resolve_field(selecto, "field_with_numbers123")
      assert field_info.name == "field_with_numbers123"
    end
    
    test "handles nil and empty field references gracefully" do
      selecto = %Selecto{
        config: %{
          source: %{fields: [], redact_fields: [], columns: %{}},
          joins: %{}
        }
      }
      
      {:error, error} = FieldResolver.resolve_field(selecto, "")
      assert %Error{type: :field_resolution_error} = error
      
      {:error, error} = FieldResolver.resolve_field(selecto, nil)
      assert %Error{type: :field_resolution_error} = error
    end
  end
end