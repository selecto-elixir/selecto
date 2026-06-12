defmodule Selecto.Temporal do
  @moduledoc false

  @date_like_types [:date, :datetime, :timestamp, :utc_datetime, :naive_datetime]

  @unix_seconds_aliases [
    :unix,
    :unix_s,
    :unix_sec,
    :unix_seconds,
    :timestamp,
    :timestamp_s,
    :timestamp_seconds
  ]

  @unix_milliseconds_aliases [
    :unix_ms,
    :unix_millis,
    :unix_milliseconds,
    :timestamp_ms,
    :timestamp_millis,
    :timestamp_milliseconds,
    :javascript,
    :javascript_epoch,
    :javascript_ms,
    :js_epoch,
    :js_ms
  ]

  @spec date_like_type(map() | nil) :: atom() | nil
  def date_like_type(nil), do: nil

  def date_like_type(conf) when is_map(conf) do
    declared_type = normalize_type(read_conf(conf, :type))

    cond do
      declared_type in @date_like_types ->
        declared_type

      presentation_type = normalize_type(read_conf(conf, :presentation_type)) ->
        if presentation_type in @date_like_types and epoch_storage(conf) != nil,
          do: presentation_type,
          else: nil

      true ->
        nil
    end
  end

  @spec date_like?(map() | nil) :: boolean()
  def date_like?(conf), do: not is_nil(date_like_type(conf))

  @spec epoch_storage(map() | nil) :: :unix_seconds | :unix_milliseconds | nil
  def epoch_storage(nil), do: nil

  def epoch_storage(conf) when is_map(conf) do
    conf
    |> read_conf(:datetime_storage)
    |> normalize_epoch_storage()
    |> case do
      nil ->
        conf
        |> read_conf(:epoch_storage)
        |> normalize_epoch_storage()

      storage ->
        storage
    end
  end

  @spec coerce_filter_value(map() | nil, term()) :: term()
  def coerce_filter_value(conf, value) when is_map(conf) do
    case {epoch_storage(conf), date_like_type(conf)} do
      {storage, type} when storage != nil and type != nil ->
        to_epoch_value(value, storage, type)

      _ ->
        value
    end
  end

  @spec coerce_sql_param_value(map() | nil, term()) :: term()
  def coerce_sql_param_value(conf, value) when is_map(conf) do
    case {epoch_storage(conf), date_like_type(conf)} do
      {storage, type} when storage != nil and type != nil ->
        to_epoch_value(value, storage, type)

      {_storage, type} when type in [:utc_datetime, :naive_datetime] ->
        to_datetime(value, type)

      _ ->
        value
    end
  end

  def coerce_sql_param_value(_conf, value), do: value

  @spec to_display_temporal(map() | nil, term()) :: term()
  def to_display_temporal(conf, value) when is_map(conf) do
    case {epoch_storage(conf), date_like_type(conf)} do
      {storage, type} when storage != nil and type != nil ->
        from_epoch_value(value, storage, type)

      _ ->
        value
    end
  end

  def to_display_temporal(_conf, value), do: value

  defp to_epoch_value(value, _storage, _type) when value in [nil, ""], do: value

  defp to_epoch_value(value, storage, type) do
    cond do
      is_integer(value) ->
        value

      is_binary(value) and String.match?(String.trim(value), ~r/^[-]?\d+$/) ->
        String.trim(value) |> String.to_integer()

      true ->
        value
        |> to_datetime(type)
        |> datetime_to_epoch(storage)
    end
  end

  defp from_epoch_value(value, _storage, _type) when value in [nil, ""], do: value

  defp from_epoch_value(value, storage, type) do
    with {:ok, integer_value} <- integer_value(value) do
      case storage do
        :unix_seconds -> DateTime.from_unix!(integer_value, :second)
        :unix_milliseconds -> DateTime.from_unix!(integer_value, :millisecond)
      end
      |> cast_display_type(type)
    else
      :error -> value
    end
  end

  defp integer_value(value) when is_integer(value), do: {:ok, value}

  defp integer_value(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer_value, ""} -> {:ok, integer_value}
      _ -> :error
    end
  end

  defp integer_value(_value), do: :error

  defp to_datetime(%DateTime{} = value, :naive_datetime), do: DateTime.to_naive(value)
  defp to_datetime(%DateTime{} = value, _type), do: value
  defp to_datetime(%NaiveDateTime{} = value, :naive_datetime), do: value
  defp to_datetime(%NaiveDateTime{} = value, _type), do: DateTime.from_naive!(value, "Etc/UTC")

  defp to_datetime(%Date{} = value, :date), do: value

  defp to_datetime(%Date{} = value, :naive_datetime) do
    NaiveDateTime.new!(value, ~T[00:00:00])
  end

  defp to_datetime(%Date{} = value, _type) do
    value
    |> NaiveDateTime.new!(~T[00:00:00])
    |> DateTime.from_naive!("Etc/UTC")
  end

  defp to_datetime(value, :date) when is_binary(value) do
    case Date.from_iso8601(String.slice(value, 0, 10)) do
      {:ok, date} -> date
      _ -> raise ArgumentError, "invalid date value #{inspect(value)}"
    end
  end

  defp to_datetime(value, type) when is_binary(value) do
    trimmed = String.trim(value)

    cond do
      match?({:ok, _, _}, DateTime.from_iso8601(trimmed)) ->
        {:ok, dt, _offset} = DateTime.from_iso8601(trimmed)

        if type == :naive_datetime, do: DateTime.to_naive(dt), else: dt

      match?({:ok, _}, NaiveDateTime.from_iso8601(trimmed)) ->
        {:ok, naive} = NaiveDateTime.from_iso8601(trimmed)
        if type == :naive_datetime, do: naive, else: DateTime.from_naive!(naive, "Etc/UTC")

      String.contains?(trimmed, " ") and
          match?({:ok, _}, NaiveDateTime.from_iso8601(String.replace(trimmed, " ", "T"))) ->
        {:ok, naive} = trimmed |> String.replace(" ", "T") |> NaiveDateTime.from_iso8601()
        if type == :naive_datetime, do: naive, else: DateTime.from_naive!(naive, "Etc/UTC")

      match?({:ok, _}, Date.from_iso8601(trimmed)) ->
        {:ok, date} = Date.from_iso8601(trimmed)
        to_datetime(date, type)

      true ->
        raise ArgumentError, "invalid datetime value #{inspect(value)}"
    end
  end

  defp to_datetime(value, _type), do: value

  defp datetime_to_epoch(%Date{} = value, storage) do
    value
    |> NaiveDateTime.new!(~T[00:00:00])
    |> DateTime.from_naive!("Etc/UTC")
    |> datetime_to_epoch(storage)
  end

  defp datetime_to_epoch(%NaiveDateTime{} = value, storage) do
    value
    |> DateTime.from_naive!("Etc/UTC")
    |> datetime_to_epoch(storage)
  end

  defp datetime_to_epoch(%DateTime{} = value, :unix_seconds), do: DateTime.to_unix(value, :second)

  defp datetime_to_epoch(%DateTime{} = value, :unix_milliseconds),
    do: DateTime.to_unix(value, :millisecond)

  defp cast_display_type(%DateTime{} = value, :date), do: DateTime.to_date(value)
  defp cast_display_type(%DateTime{} = value, :naive_datetime), do: DateTime.to_naive(value)
  defp cast_display_type(%DateTime{} = value, _type), do: value

  defp normalize_type(value) when value in @date_like_types, do: value
  defp normalize_type(:utc_datetime_usec), do: :utc_datetime
  defp normalize_type(:naive_datetime_usec), do: :naive_datetime
  defp normalize_type(_value), do: nil

  defp normalize_epoch_storage(value) when value in @unix_seconds_aliases, do: :unix_seconds

  defp normalize_epoch_storage(value) when value in @unix_milliseconds_aliases,
    do: :unix_milliseconds

  defp normalize_epoch_storage(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.downcase()
    |> String.replace("-", "_")
    |> String.replace(" ", "_")
    |> then(fn normalized ->
      cond do
        normalized in Enum.map(@unix_seconds_aliases, &Atom.to_string/1) ->
          :unix_seconds

        normalized in Enum.map(@unix_milliseconds_aliases, &Atom.to_string/1) ->
          :unix_milliseconds

        true ->
          nil
      end
    end)
  end

  defp normalize_epoch_storage(_value), do: nil

  defp read_conf(conf, key) do
    Map.get(conf, key, Map.get(conf, Atom.to_string(key)))
  end
end
