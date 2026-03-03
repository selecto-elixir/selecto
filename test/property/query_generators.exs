defmodule Selecto.Property.QueryGenerators do
  @moduledoc false

  import StreamData

  @fields ["id", "name", "age", "active"]

  @spec default_domain(String.t()) :: map()
  def default_domain(source_table \\ "selecto_property_users") do
    %{
      name: "PropertyUsers",
      source: %{
        source_table: source_table,
        primary_key: :id,
        fields: [:id, :name, :age, :active],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          age: %{type: :integer},
          active: %{type: :boolean}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  @spec query_spec_generator() :: StreamData.t()
  def query_spec_generator do
    fixed_map(%{
      selected: field_list_generator(),
      filters: list_of(filter_generator(), max_length: 3),
      order_by: list_of(order_spec_generator(), max_length: 2),
      limit: optional_int_generator(0, 25),
      offset: optional_int_generator(0, 25)
    })
  end

  @spec operation_generator() :: StreamData.t()
  def operation_generator do
    one_of([
      map(field_list_generator(3), &{:select, &1}),
      map(filter_generator(), &{:filter, &1}),
      map(order_spec_generator(), &{:order_by, &1}),
      map(field_generator(), &{:group_by, &1}),
      map(integer(0..25), &{:limit, &1}),
      map(integer(0..25), &{:offset, &1})
    ])
  end

  @spec invalid_field_generator() :: StreamData.t()
  def invalid_field_generator do
    string(:alphanumeric, min_length: 1, max_length: 12)
    |> map(&("bad_" <> &1))
  end

  @spec simple_value_generator() :: StreamData.t()
  def simple_value_generator do
    one_of([
      integer(-1000..1000),
      boolean(),
      string(:alphanumeric, min_length: 1, max_length: 8)
    ])
  end

  defp field_generator, do: member_of(@fields)

  defp filter_generator do
    one_of([
      map(integer(1..10_000), &{"id", &1}),
      map(string(:alphanumeric, min_length: 1, max_length: 12), &{"name", &1}),
      map(integer(0..120), &{"age", &1}),
      map(boolean(), &{"active", &1}),
      map(integer(0..120), &{"age", {:gt, &1}}),
      map(integer(0..120), &{"age", {:lt, &1}}),
      map(string(:alphanumeric, min_length: 1, max_length: 6), &{"name", {:ilike, "%#{&1}%"}})
    ])
  end

  defp order_spec_generator do
    one_of([
      field_generator(),
      map(field_generator(), &{:asc, &1}),
      map(field_generator(), &{:desc, &1})
    ])
  end

  defp optional_int_generator(min, max) do
    one_of([constant(nil), integer(min..max)])
  end

  defp field_list_generator(max_length \\ 4) do
    list_of(field_generator(), min_length: 1, max_length: max_length)
    |> map(&Enum.uniq/1)
  end
end
