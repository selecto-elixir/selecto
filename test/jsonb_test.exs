defmodule Selecto.JsonbTest do
  use ExUnit.Case, async: true

  alias Selecto.Jsonb

  defp domain do
    %{
      columns: %{
        "attributes" => %{
          type: :jsonb,
          schema: %{
            "color" => %{type: :string},
            "weight" => %{type: :decimal},
            "dimensions" => %{
              type: :object,
              schema: %{"length" => %{type: :decimal}, "width" => %{type: :decimal}}
            },
            "items" => %{
              type: :array,
              items: %{type: :object, schema: %{"sku" => %{type: :string}}}
            },
            "tags" => %{type: :array, items: %{type: :string}}
          }
        },
        "name" => %{type: :string}
      }
    }
  end

  test "parse_field_reference distinguishes jsonb paths" do
    assert {:jsonb, "attributes", ["color"]} =
             Jsonb.parse_field_reference("attributes.color", domain())

    assert {:jsonb, "attributes", ["dimensions", "length"]} =
             Jsonb.parse_field_reference("attributes.dimensions.length", domain())

    assert {:regular, "name"} = Jsonb.parse_field_reference("name", domain())
    assert {:regular, "users.name"} = Jsonb.parse_field_reference("users.name", domain())
    assert {:regular, :name} = Jsonb.parse_field_reference(:name, domain())
  end

  test "get_path_schema resolves nested object and array object paths" do
    assert %{type: :string} = Jsonb.get_path_schema(domain(), "attributes", ["color"])

    assert %{type: :decimal} =
             Jsonb.get_path_schema(domain(), "attributes", ["dimensions", "length"])

    assert %{type: :string} = Jsonb.get_path_schema(domain(), "attributes", ["items", "sku"])
    assert nil == Jsonb.get_path_schema(domain(), "attributes", ["missing"])
    assert nil == Jsonb.get_path_schema(domain(), "name", ["x"])
  end

  test "build_extraction supports alias path operators and casts" do
    assert ~s("attributes"->>'color') == Jsonb.build_extraction("attributes", ["color"])

    assert ~s("u"."attributes"#>'{dimensions,length}') ==
             Jsonb.build_extraction("attributes", ["dimensions", "length"],
               as_text: false,
               table_alias: "u"
             )

    assert "(\"attributes\"#>>'{dimensions,length}')::numeric" ==
             Jsonb.build_extraction("attributes", ["dimensions", "length"], cast: :decimal)
  end

  test "build_extraction supports mssql json value and query functions" do
    assert "JSON_VALUE(u.attributes, '$.color')" ==
             Jsonb.build_extraction("attributes", ["color"],
               adapter: SelectoDBMSSQL.Adapter,
               table_alias: "u"
             )

    assert "JSON_QUERY(u.attributes, '$.dimensions')" ==
             Jsonb.build_extraction("attributes", ["dimensions"],
               adapter: SelectoDBMSSQL.Adapter,
               table_alias: "u",
               as_text: false
             )

    assert "CAST(JSON_VALUE(u.attributes, '$.dimensions.length') AS decimal(38, 10))" ==
             Jsonb.build_extraction("attributes", ["dimensions", "length"],
               adapter: SelectoDBMSSQL.Adapter,
               table_alias: "u",
               cast: :decimal
             )
  end

  test "build_contains and key existence expressions" do
    contains = Jsonb.build_contains("attributes", %{"color" => "red"}, table_alias: "u")
    assert String.contains?(contains, ~s("u"."attributes" @>))
    assert String.contains?(contains, "::jsonb")

    assert ~s("attributes" ? 'color') == Jsonb.build_key_exists("attributes", "color")
    assert ~s("attributes" ? 'color') == Jsonb.build_key_exists("attributes", ["color"])

    nested = Jsonb.build_key_exists("attributes", ["dimensions", "length"])
    assert String.contains?(nested, "? 'length'")
    assert String.contains?(nested, "->'dimensions'")
  end

  test "mssql contains and key existence use json functions" do
    contains =
      Jsonb.build_contains("attributes", %{"color" => "red", "dimensions" => %{"length" => 5}},
        adapter: SelectoDBMSSQL.Adapter,
        table_alias: "u"
      )

    assert IO.iodata_to_binary(contains) =~ "JSON_VALUE(u.attributes, '$.color') = 'red'"

    assert IO.iodata_to_binary(contains) =~
             "JSON_VALUE(u.attributes, '$.dimensions.length') = '5'"

    exists =
      Jsonb.build_key_exists("attributes", ["dimensions", "length"],
        adapter: SelectoDBMSSQL.Adapter,
        table_alias: "u"
      )

    assert exists =~ "JSON_QUERY(u.attributes, '$.dimensions.length') IS NOT NULL"
    assert exists =~ "JSON_VALUE(u.attributes, '$.dimensions.length') IS NOT NULL"
  end

  test "mssql json array helpers use openjson" do
    one =
      Jsonb.build_array_contains("attributes", ["tags"], "featured",
        adapter: SelectoDBMSSQL.Adapter,
        table_alias: "u"
      )

    all =
      Jsonb.build_array_contains_all("attributes", ["tags"], ["featured", "new"],
        adapter: SelectoDBMSSQL.Adapter,
        table_alias: "u"
      )

    assert IO.iodata_to_binary(one) =~ "OPENJSON(u.attributes, '$.tags')"
    assert IO.iodata_to_binary(one) =~ "WHERE value = 'featured'"
    assert IO.iodata_to_binary(all) =~ "OPENJSON(u.attributes, '$.tags')"
    assert IO.iodata_to_binary(all) =~ "WHERE value = 'featured'"
    assert IO.iodata_to_binary(all) =~ "WHERE value = 'new'"
  end

  test "array contains helpers" do
    one = Jsonb.build_array_contains("attributes", ["tags"], "featured")
    many = Jsonb.build_array_contains("attributes", ["tags"], ["featured", "new"])
    all = Jsonb.build_array_contains_all("attributes", ["tags"], ["featured", "new"])

    assert String.contains?(one, " ? 'featured'")
    assert String.contains?(many, "?| array['featured','new']")
    assert String.contains?(all, "?& array['featured','new']")
  end

  test "cast mapping and jsonb column detection" do
    assert nil == Jsonb.pg_cast_for_type(:string)
    assert :integer == Jsonb.pg_cast_for_type(:integer)
    assert :decimal == Jsonb.pg_cast_for_type(:decimal)
    assert :float == Jsonb.pg_cast_for_type(:float)
    assert :boolean == Jsonb.pg_cast_for_type(:boolean)
    assert :date == Jsonb.pg_cast_for_type(:date)
    assert :datetime == Jsonb.pg_cast_for_type(:naive_datetime)
    assert :utc_datetime == Jsonb.pg_cast_for_type(:utc_datetime)
    assert nil == Jsonb.pg_cast_for_type(:unknown)

    assert Jsonb.jsonb_column?(domain(), "attributes")
    refute Jsonb.jsonb_column?(domain(), "name")
    refute Jsonb.jsonb_column?(domain(), "missing")
  end
end
