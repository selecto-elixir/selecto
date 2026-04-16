defmodule Selecto.Presentation do
  @moduledoc false

  alias Selecto.Temporal

  @semantic_types %{
    "measurement" => :measurement,
    "temporal" => :temporal,
    "number" => :number
  }

  @temporal_kinds %{
    "instant" => :instant,
    "local_date" => :local_date,
    "local_time" => :local_time,
    "naive_datetime" => :naive_datetime
  }

  @known_quantities %{
    "temperature" => :temperature,
    "length" => :length,
    "distance" => :distance,
    "area" => :area,
    "volume" => :volume,
    "mass" => :mass,
    "weight" => :mass,
    "speed" => :speed
  }

  @known_units %{
    "c" => :celsius,
    "celsius" => :celsius,
    "f" => :fahrenheit,
    "fahrenheit" => :fahrenheit,
    "k" => :kelvin,
    "kelvin" => :kelvin,
    "mm" => :millimeter,
    "millimeter" => :millimeter,
    "millimeters" => :millimeter,
    "cm" => :centimeter,
    "centimeter" => :centimeter,
    "centimeters" => :centimeter,
    "m" => :meter,
    "meter" => :meter,
    "meters" => :meter,
    "km" => :kilometer,
    "kilometer" => :kilometer,
    "kilometers" => :kilometer,
    "in" => :inch,
    "inch" => :inch,
    "inches" => :inch,
    "ft" => :foot,
    "foot" => :foot,
    "feet" => :foot,
    "yd" => :yard,
    "yard" => :yard,
    "yards" => :yard,
    "mi" => :mile,
    "mile" => :mile,
    "miles" => :mile,
    "g" => :gram,
    "gram" => :gram,
    "grams" => :gram,
    "kg" => :kilogram,
    "kilogram" => :kilogram,
    "kilograms" => :kilogram,
    "oz" => :ounce,
    "ounce" => :ounce,
    "ounces" => :ounce,
    "lb" => :pound,
    "lbs" => :pound,
    "pound" => :pound,
    "pounds" => :pound,
    "ml" => :milliliter,
    "milliliter" => :milliliter,
    "milliliters" => :milliliter,
    "l" => :liter,
    "liter" => :liter,
    "liters" => :liter,
    "floz" => :fluid_ounce,
    "fluid_ounce" => :fluid_ounce,
    "fluid_ounces" => :fluid_ounce,
    "gallon" => :gallon,
    "gallons" => :gallon,
    "m2" => :square_meter,
    "square_meter" => :square_meter,
    "square_meters" => :square_meter,
    "sq_m" => :square_meter,
    "ft2" => :square_foot,
    "square_foot" => :square_foot,
    "square_feet" => :square_foot,
    "sq_ft" => :square_foot,
    "acre" => :acre,
    "acres" => :acre,
    "hectare" => :hectare,
    "hectares" => :hectare,
    "mps" => :meter_per_second,
    "meter_per_second" => :meter_per_second,
    "meters_per_second" => :meter_per_second,
    "kph" => :kilometer_per_hour,
    "kmh" => :kilometer_per_hour,
    "kilometer_per_hour" => :kilometer_per_hour,
    "kilometers_per_hour" => :kilometer_per_hour,
    "mph" => :mile_per_hour,
    "mile_per_hour" => :mile_per_hour,
    "miles_per_hour" => :mile_per_hour
  }

  @spec normalize_column(map() | nil) :: map() | nil
  def normalize_column(nil), do: nil

  def normalize_column(%{} = column) do
    case normalize_presentation(column) do
      nil ->
        column
        |> Map.delete(:presentation)
        |> Map.delete("presentation")

      presentation ->
        Map.put(column, :presentation, presentation)
    end
  end

  @spec normalize_presentation(map() | nil) :: map() | nil
  def normalize_presentation(nil), do: nil

  def normalize_presentation(%{} = column) do
    explicit =
      column
      |> read_conf(:presentation)
      |> normalize_presentation_map()

    presentation =
      derived_temporal_presentation(column)
      |> Map.merge(explicit)
      |> finalize_presentation()

    if map_size(presentation) == 0, do: nil, else: presentation
  end

  @spec presentation(map() | nil) :: map() | nil
  def presentation(nil), do: nil
  def presentation(%{} = column), do: normalize_presentation(column)

  @spec measurement?(map() | nil) :: boolean()
  def measurement?(column), do: match?(%{semantic_type: :measurement}, presentation(column))

  @spec temporal?(map() | nil) :: boolean()
  def temporal?(column), do: match?(%{semantic_type: :temporal}, presentation(column))

  @spec quantity(map() | nil) :: atom() | String.t() | nil
  def quantity(column), do: presentation(column) |> maybe_get(:quantity, nil)

  @spec canonical_unit(map() | nil) :: atom() | String.t() | nil
  def canonical_unit(column), do: presentation(column) |> maybe_get(:canonical_unit, nil)

  @spec available_units(map() | nil) :: [atom() | String.t()]
  def available_units(column), do: presentation(column) |> maybe_get(:available_units, [])

  @spec default_unit(map() | nil) :: atom() | String.t() | nil
  def default_unit(column), do: presentation(column) |> maybe_get(:default_unit, nil)

  @spec temporal_kind(map() | nil) :: atom() | nil
  def temporal_kind(column), do: presentation(column) |> maybe_get(:temporal_kind, nil)

  defp normalize_presentation_map(nil), do: %{}
  defp normalize_presentation_map(map) when map == %{}, do: %{}

  defp normalize_presentation_map(map) when is_map(map) do
    %{}
    |> maybe_put(:semantic_type, normalize_semantic_type(read_conf(map, :semantic_type)))
    |> maybe_put(:quantity, normalize_quantity(read_conf(map, :quantity)))
    |> maybe_put(:canonical_unit, normalize_unit(read_conf(map, :canonical_unit)))
    |> maybe_put(:available_units, normalize_units(read_conf(map, :available_units)))
    |> maybe_put(:default_unit, normalize_unit(read_conf(map, :default_unit)))
    |> maybe_put(:temporal_kind, normalize_temporal_kind(read_conf(map, :temporal_kind)))
    |> maybe_put(:storage_timezone, normalize_timezone(read_conf(map, :storage_timezone)))
    |> maybe_put(:display_timezone, normalize_display_timezone(read_conf(map, :display_timezone)))
    |> maybe_put(:format, normalize_format_map(read_conf(map, :format)))
  end

  defp normalize_presentation_map(_other), do: %{}

  defp derived_temporal_presentation(column) do
    case Temporal.date_like_type(column) do
      :date ->
        %{semantic_type: :temporal, temporal_kind: :local_date}

      type when type in [:utc_datetime, :datetime] ->
        %{
          semantic_type: :temporal,
          temporal_kind: :instant,
          storage_timezone: "Etc/UTC",
          display_timezone: :viewer
        }

      :naive_datetime ->
        %{semantic_type: :temporal, temporal_kind: :naive_datetime}

      _ ->
        %{}
    end
  end

  defp finalize_presentation(presentation) do
    presentation
    |> ensure_default_unit()
    |> ensure_available_units()
  end

  defp ensure_default_unit(%{canonical_unit: canonical_unit} = presentation)
       when not is_nil(canonical_unit) do
    Map.put_new(presentation, :default_unit, canonical_unit)
  end

  defp ensure_default_unit(presentation), do: presentation

  defp ensure_available_units(%{available_units: available_units} = presentation)
       when is_list(available_units) and available_units != [] do
    extra_units =
      [Map.get(presentation, :canonical_unit), Map.get(presentation, :default_unit)]
      |> Enum.reject(&is_nil/1)

    Map.put(presentation, :available_units, Enum.uniq(available_units ++ extra_units))
  end

  defp ensure_available_units(%{canonical_unit: canonical_unit} = presentation)
       when not is_nil(canonical_unit) do
    default_unit = Map.get(presentation, :default_unit, canonical_unit)
    Map.put(presentation, :available_units, Enum.uniq([canonical_unit, default_unit]))
  end

  defp ensure_available_units(presentation), do: presentation

  defp normalize_semantic_type(value) when is_atom(value), do: value

  defp normalize_semantic_type(value) when is_binary(value),
    do: Map.get(@semantic_types, normalize_key(value))

  defp normalize_semantic_type(_value), do: nil

  defp normalize_temporal_kind(value) when is_atom(value), do: value

  defp normalize_temporal_kind(value) when is_binary(value),
    do: Map.get(@temporal_kinds, normalize_key(value))

  defp normalize_temporal_kind(_value), do: nil

  defp normalize_quantity(value) when is_atom(value),
    do: Map.get(@known_quantities, Atom.to_string(value), value)

  defp normalize_quantity(value) when is_binary(value),
    do: Map.get(@known_quantities, normalize_key(value), String.trim(value))

  defp normalize_quantity(_value), do: nil

  defp normalize_unit(value) when is_atom(value),
    do: Map.get(@known_units, Atom.to_string(value), value)

  defp normalize_unit(value) when is_binary(value),
    do: Map.get(@known_units, normalize_key(value), String.trim(value))

  defp normalize_unit(_value), do: nil

  defp normalize_units(values) when is_list(values) do
    values
    |> Enum.map(&normalize_unit/1)
    |> Enum.reject(&is_nil/1)
  end

  defp normalize_units(_value), do: []

  defp normalize_timezone(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_timezone(_value), do: nil

  defp normalize_display_timezone(value) when value in [:viewer, :user], do: :viewer

  defp normalize_display_timezone(value) when is_binary(value) do
    case normalize_key(value) do
      "viewer" -> :viewer
      "user" -> :viewer
      other when other != "" -> String.trim(value)
      _ -> nil
    end
  end

  defp normalize_display_timezone(_value), do: nil

  defp normalize_format_map(value) when is_map(value), do: value
  defp normalize_format_map(_value), do: nil

  defp normalize_key(value) do
    value
    |> String.trim()
    |> String.downcase()
    |> String.replace(~r/[\s-]+/u, "_")
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, _key, []), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_get(nil, _key, default), do: default
  defp maybe_get(map, key, default), do: Map.get(map, key, default)

  defp read_conf(conf, key) do
    Map.get(conf, key, Map.get(conf, Atom.to_string(key)))
  end
end
