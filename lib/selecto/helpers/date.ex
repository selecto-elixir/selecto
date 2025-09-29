defmodule Selecto.Helpers.Date do

  @moduledoc """
  Date helper functions for Selecto.

  Uses half-open intervals (>= start AND < end) for datetime ranges
  to avoid precision issues with timestamps.
  """

  # Handle nil case when regex doesn't match
  defp expand_date(nil) do
    # Default to today if pattern doesn't match
    start = Timex.now() |> Timex.beginning_of_day()
    {start, start |> Timex.shift(days: 1)}
  end

  defp expand_date(%{"year"=>year, "month"=>"", "day"=>""}) do
    start = Timex.to_datetime({{String.to_integer(year), 1, 1},{0,0,0}},  "Etc/UTC")
    # Use start of next year instead of end of current year
    stop = start |> Timex.shift(years: 1)
    {start, stop}
  end

  defp expand_date(%{"year"=>year, "month"=>month, "day"=>""}) do
    start = Timex.to_datetime({{String.to_integer(year), String.to_integer(month), 1},{0,0,0}},  "Etc/UTC")
    # Use start of next month instead of end of current month
    stop = start |> Timex.shift(months: 1)
    {start, stop}
  end

  defp expand_date(%{"year"=>year, "month"=>month, "day"=>day}) do
    start = Timex.to_datetime({{String.to_integer(year), String.to_integer(month), String.to_integer(day)},{0,0,0}}, "Etc/UTC")
    # Use start of next day instead of end of current day
    stop = start |> Timex.shift(days: 1)
    {start, stop}
  end

  # Catch-all for unexpected input
  defp expand_date(_other) do
    # Default to today if pattern doesn't match expected format
    start = Timex.now() |> Timex.beginning_of_day()
    {start, start |> Timex.shift(days: 1)}
  end


  # Convert various date formats to DateTime
  defp proc_date(%NaiveDateTime{} = date) do
    DateTime.from_naive!(date, "Etc/UTC")
  end

  defp proc_date(%DateTime{} = date) do
    date
  end

  defp proc_date(date) when is_binary(date) do
    date = cond do
      Regex.match?(~r/Z$/, date) -> date
      Regex.match?(~r/\d\d:\d\d:\d\d/, date) -> date <> "Z"   #Weird...
      Regex.match?(~r/\d\d:\d\d/, date) -> date <> ":00Z"
      true -> date
    end
    #IO.inspect(date, label: "Parsing...")
    {:ok, value, _} = DateTime.from_iso8601(date)
    value
  end


  def val_to_dates(%{"value" => "today", "value2" => ""}) do
    start = Timex.now() |> Timex.beginning_of_day()
    # Use start of tomorrow instead of end of today
    {start, start |> Timex.shift(days: 1)}
  end
  def val_to_dates(%{"value" => "tomorrow", "value2" => ""}) do
    start = Timex.now() |> Timex.shift(days: 1) |> Timex.beginning_of_day()
    # Use start of day after tomorrow
    {start, start |> Timex.shift(days: 1)}
  end
  def val_to_dates(%{"value" => "yesterday", "value2" => ""}) do
    start = Timex.now() |> Timex.shift(days: -1) |> Timex.beginning_of_day()
    # Use start of today
    {start, start |> Timex.shift(days: 1)}
  end
  def val_to_dates(%{"value" => "this_week", "value2" => ""}) do
    start = Timex.now() |> Timex.beginning_of_week()
    # Use start of next week
    {start, start |> Timex.shift(weeks: 1)}
  end
  def val_to_dates(%{"value" => "last_week", "value2" => ""}) do
    start = Timex.now() |> Timex.shift(weeks: -1) |> Timex.beginning_of_week()
    # Use start of this week
    {start, start |> Timex.shift(weeks: 1)}
  end
  def val_to_dates(%{"value" => "this_month", "value2" => ""}) do
    start = Timex.now() |> Timex.beginning_of_month()
    # Use start of next month
    {start, start |> Timex.shift(months: 1)}
  end
  def val_to_dates(%{"value" => "last_month", "value2" => ""}) do
    start = Timex.now() |> Timex.shift(months: -1) |> Timex.beginning_of_month()
    # Use start of this month
    {start, start |> Timex.shift(months: 1)}
  end
  def val_to_dates(%{"value" => "this_year", "value2" => ""}) do
    start = Timex.now() |> Timex.beginning_of_year()
    # Use start of next year
    {start, start |> Timex.shift(years: 1)}
  end
  def val_to_dates(%{"value" => "last_year", "value2" => ""}) do
    start = Timex.now() |> Timex.shift(years: -1) |> Timex.beginning_of_year()
    # Use start of this year
    {start, start |> Timex.shift(years: 1)}
  end
  def val_to_dates(%{"value" => "this_quarter", "value2" => ""}) do
    start = Timex.now() |> Timex.beginning_of_quarter()
    # Use start of next quarter
    {start, start |> Timex.shift(months: 3)}
  end
  def val_to_dates(%{"value" => "last_quarter", "value2" => ""}) do
    start = Timex.now() |> Timex.shift(months: -3) |> Timex.beginning_of_quarter()
    # Use start of this quarter
    {start, start |> Timex.shift(months: 3)}
  end

  def val_to_dates(%{"value" => v1, "value2" => ""}) do
    Regex.named_captures(~r/(?<year>\d{4})-?(?<month>\d{2})?-?(?<day>\d{2})?/, v1) |> expand_date()
  end

  def val_to_dates(%{"value" => v1, "value2" => v2}) do
    {proc_date(v1), proc_date(v2)}
  end

end
