defmodule Selecto.Helpers.Date do

  defp expand_date(%{"year"=>year, "month"=>"", "day"=>""}) do
    start = Timex.to_datetime({{String.to_integer(year), 1, 1},{0,0,0}},  "Etc/UTC")
    stop = Timex.end_of_year(start)
    {start, stop}
  end

  defp expand_date(%{"year"=>year, "month"=>month, "day"=>""}) do
    start = Timex.to_datetime({{String.to_integer(year), String.to_integer(month), 1},{0,0,0}},  "Etc/UTC")
    stop = Timex.end_of_month(start)
    {start, stop}
  end

  defp expand_date(%{"year"=>year, "month"=>month, "day"=>day}) do
    start = Timex.to_datetime({{String.to_integer(year), String.to_integer(month), String.to_integer(day)},{0,0,0}}, "Etc/UTC")
    stop = Timex.end_of_day(start)
    {start, stop}
  end

  defp proc_date(date) do ### do this better TODO
    {:ok, value, i} = DateTime.from_iso8601(date <> ":00Z")
    value
  end

  def val_to_dates(%{"value" => "today", "value2" => ""}) do
    start = Timex.now() |> Timex.beginning_of_day()
    {start, Timex.end_of_day(start)}
  end
  def val_to_dates(%{"value" => "tomorrow", "value2" => ""}) do
    start = Timex.now() |> Timex.shift(days: 1) |> Timex.beginning_of_day()
    {start, Timex.end_of_day(start)}
  end
  ### TODO more of these....

  def val_to_dates(%{"value" => v1, "value2" => ""}) do
    Regex.named_captures(~r/(?<year>\d{4})-(?<month>\d{2})-?(?<day>\d{2})?/, v1) |> expand_date()
  end

  def val_to_dates(%{"value" => v1, "value2" => v2}) do
    {proc_date(v1), proc_date(v2)}
  end

end
