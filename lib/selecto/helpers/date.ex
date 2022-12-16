defmodule Selecto.Helpers.Date do


  ### TODO do we need to set nanoseconds etc or should we switch to a Start >= v < End instead of Between?!?!?
  ### TODO time zones?!?!?!

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


  defp proc_date(%NaiveDateTime{} = date) do ### do this better TODO
    date
  end

  defp proc_date(%DateTime{} = date) do ### do this better TODO
    date
  end

  defp proc_date(date) when is_binary(date) do ### do this better TODO
    date = cond do
      Regex.match?(~r/Z$/, date) -> date
      Regex.match?(~r/\d\d:\d\d:\d\d/, date) -> date <> "Z"   #Weird...
      Regex.match?(~r/\d\d:\d\d/, date) -> date <> ":00Z"
      true -> date
    end
    #IO.inspect(date, label: "Parsing...")
    {:ok, value, i} = DateTime.from_iso8601(date)
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

  def val_to_dates(%{"value" => v1, "value2" => v2} = f) do
    #IO.inspect(f)
    {proc_date(v1), proc_date(v2)}
  end

end
