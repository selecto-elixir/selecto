defmodule Selecto.Helpers.DateTest do
  use ExUnit.Case
  alias Selecto.Helpers.Date

  # Skip these tests if Timex is not available
  setup do
    try do
      Code.ensure_loaded!(Timex)
      :ok
    rescue
      ArgumentError -> :skip
    end
  end

  describe "val_to_dates/1" do
    test "handles 'today' value" do
      input = %{"value" => "today", "value2" => ""}
      
      {start_date, end_date} = Date.val_to_dates(input)
      
      assert %DateTime{} = start_date
      assert %DateTime{} = end_date
      # Should be start and end of the same day
      assert start_date.hour == 0
      assert end_date.hour == 23
    end

    test "handles 'tomorrow' value" do
      input = %{"value" => "tomorrow", "value2" => ""}
      
      {start_date, end_date} = Date.val_to_dates(input)
      
      assert %DateTime{} = start_date
      assert %DateTime{} = end_date
    end

    test "handles year-only input" do
      input = %{"value" => "2023", "value2" => ""}
      
      {start_date, end_date} = Date.val_to_dates(input)
      
      assert %DateTime{} = start_date
      assert %DateTime{} = end_date
      assert start_date.year == 2023
      assert start_date.month == 1
      assert start_date.day == 1
    end

    test "handles year-month input" do
      input = %{"value" => "2023-06", "value2" => ""}
      
      {start_date, end_date} = Date.val_to_dates(input)
      
      assert %DateTime{} = start_date
      assert %DateTime{} = end_date
      assert start_date.year == 2023
      assert start_date.month == 6
      assert start_date.day == 1
    end

    test "handles full date input" do
      input = %{"value" => "2023-06-15", "value2" => ""}
      
      {start_date, end_date} = Date.val_to_dates(input)
      
      assert %DateTime{} = start_date
      assert %DateTime{} = end_date
      assert start_date.year == 2023
      assert start_date.month == 6
      assert start_date.day == 15
    end

    test "handles date range input" do
      input = %{"value" => "2023-01-01T00:00:00Z", "value2" => "2023-12-31T23:59:59Z"}
      
      {start_date, end_date} = Date.val_to_dates(input)
      
      assert %DateTime{} = start_date
      assert %DateTime{} = end_date
      assert start_date.year == 2023
      assert start_date.month == 1
      assert end_date.year == 2023
      assert end_date.month == 12
    end
  end
end