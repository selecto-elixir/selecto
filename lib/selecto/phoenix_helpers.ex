defmodule Selecto.PhoenixHelpers do
  @moduledoc """
  Phoenix-specific helper functions for Selecto integration.
  
  This module provides helpers for common Phoenix patterns like
  LiveView integration, form helpers, and URL parameter handling.
  """

  @doc """
  Extract Selecto configuration from Phoenix controller/LiveView params.
  
  This function parses URL parameters and session data to restore
  a Selecto query state, useful for bookmarkable URLs and pagination.
  
  ## Parameters
  
  - `selecto` - Base Selecto configuration
  - `params` - Phoenix params map (from controller or LiveView)
  - `opts` - Options for parameter parsing
  
  ## Options
  
  - `:page_param` - Parameter name for pagination (default: "page")
  - `:per_page_param` - Parameter name for items per page (default: "per_page") 
  - `:sort_param` - Parameter name for sorting (default: "sort")
  - `:filter_param` - Parameter name for filters (default: "filter")
  - `:default_per_page` - Default items per page (default: 20)
  
  ## Examples
  
      # In a LiveView
      def handle_params(params, _uri, socket) do
        selecto = socket.assigns.selecto
        |> Selecto.PhoenixHelpers.from_params(params)
        
        {:noreply, assign(socket, selecto: selecto)}
      end
      
      # In a controller
      def index(conn, params) do
        selecto = Selecto.EctoAdapter.configure(Repo, User)
        |> Selecto.PhoenixHelpers.from_params(params)
        
        {:ok, {users, columns, aliases}} = Selecto.execute(selecto)
        render(conn, "index.html", users: users)
      end
  """
  def from_params(selecto, params, opts \\ []) do
    page_param = Keyword.get(opts, :page_param, "page")
    per_page_param = Keyword.get(opts, :per_page_param, "per_page")
    sort_param = Keyword.get(opts, :sort_param, "sort")
    filter_param = Keyword.get(opts, :filter_param, "filter")
    default_per_page = Keyword.get(opts, :default_per_page, 20)
    
    selecto
    |> apply_pagination(params, page_param, per_page_param, default_per_page)
    |> apply_sorting(params, sort_param)
    |> apply_filters(params, filter_param)
  end

  @doc """
  Convert Selecto configuration to URL parameters.
  
  Useful for generating bookmarkable URLs and maintaining state
  across page transitions.
  
  ## Examples
  
      # In a template
      <%= link("Next Page", to: Routes.user_path(@conn, :index, 
            Selecto.PhoenixHelpers.to_params(@selecto, page: @page + 1))) %>
  """
  def to_params(selecto, additional_params \\ %{}) do
    # Extract current state from selecto
    %{}
    |> add_pagination_params(selecto)
    |> add_sorting_params(selecto)
    |> add_filter_params(selecto)
    |> Map.merge(additional_params)
  end

  @doc """
  Generate pagination info for templates.
  
  Returns a map with pagination metadata useful for rendering
  page controls in templates.
  
  ## Examples
  
      # In a LiveView
      pagination = Selecto.PhoenixHelpers.pagination_info(selecto, total_count)
      
      # Returns:
      # %{
      #   current_page: 2,
      #   per_page: 20,
      #   total_pages: 5,
      #   total_count: 89,
      #   has_prev: true,
      #   has_next: true,
      #   prev_page: 1,
      #   next_page: 3
      # }
  """
  def pagination_info(selecto, total_count, opts \\ []) do
    default_per_page = Keyword.get(opts, :default_per_page, 20)
    
    # Extract pagination from selecto state (this would need to be added to Selecto core)
    current_page = get_current_page(selecto, 1)
    per_page = get_per_page(selecto, default_per_page)
    
    total_pages = ceil(total_count / per_page)
    
    %{
      current_page: current_page,
      per_page: per_page,
      total_pages: total_pages,
      total_count: total_count,
      has_prev: current_page > 1,
      has_next: current_page < total_pages,
      prev_page: max(current_page - 1, 1),
      next_page: min(current_page + 1, total_pages)
    }
  end

  @doc """
  Generate form helpers for Selecto filters.
  
  Creates form field data structures that can be used with
  Phoenix form helpers to build dynamic filter forms.
  
  ## Examples
  
      # In a template
      <.form for={@form} phx-change="filter">
        <%= for field <- Selecto.PhoenixHelpers.filter_fields(@selecto) do %>
          <div class="field">
            <%= label(@form, field.name) %>
            <%= case field.type do %>
              <% :string -> %>
                <%= text_input(@form, field.name, value: field.current_value) %>
              <% :select -> %>
                <%= select(@form, field.name, field.options, selected: field.current_value) %>
            <% end %>
          </div>
        <% end %>
      </.form>
  """
  def filter_fields(selecto) do
    columns = Selecto.columns(selecto)
    
    Enum.map(columns, fn {field_name, column_config} ->
      %{
        name: field_name,
        label: Map.get(column_config, :name, humanize(field_name)),
        type: column_type_to_input_type(column_config.type),
        current_value: get_current_filter_value(selecto, field_name),
        options: get_field_options(column_config)
      }
    end)
  end

  @doc """
  Apply LiveView-style updates to Selecto configuration.
  
  Useful for handling LiveView events that modify the query.
  
  ## Examples
  
      # In LiveView event handlers
      def handle_event("filter", %{"user" => filter_params}, socket) do
        selecto = Selecto.PhoenixHelpers.update_selecto(
          socket.assigns.selecto,
          :filter,
          filter_params
        )
        
        {:noreply, assign(socket, selecto: selecto)}
      end
  """
  def update_selecto(selecto, :filter, params) do
    Enum.reduce(params, selecto, fn {field, value}, acc ->
      if value != nil and value != "" do
        Selecto.filter(acc, {field, value})
      else
        remove_filter(acc, field)
      end
    end)
  end
  
  def update_selecto(selecto, :sort, field) do
    Selecto.order_by(selecto, [field])
  end
  
  def update_selecto(selecto, :page, page) when is_binary(page) do
    update_selecto(selecto, :page, String.to_integer(page))
  end
  
  def update_selecto(selecto, :page, page) do
    # This would need to be implemented in Selecto core
    # For now, we'll store it in a custom field
    put_pagination(selecto, :page, page)
  end

  ## Private helper functions

  defp apply_pagination(selecto, params, page_param, per_page_param, default_per_page) do
    page = get_integer_param(params, page_param, 1)
    per_page = get_integer_param(params, per_page_param, default_per_page)
    
    selecto
    |> put_pagination(:page, page)
    |> put_pagination(:per_page, per_page)
  end

  defp apply_sorting(selecto, params, sort_param) do
    case Map.get(params, sort_param) do
      nil -> selecto
      "" -> selecto
      sort_field -> Selecto.order_by(selecto, [sort_field])
    end
  end

  defp apply_filters(selecto, params, filter_param) do
    case Map.get(params, filter_param) do
      nil -> selecto
      filters when is_map(filters) ->
        Enum.reduce(filters, selecto, fn {field, value}, acc ->
          if value != nil and value != "" do
            Selecto.filter(acc, {field, value})
          else
            acc
          end
        end)
      _ -> selecto
    end
  end

  defp add_pagination_params(params, selecto) do
    params
    |> Map.put("page", get_current_page(selecto, 1))
    |> Map.put("per_page", get_per_page(selecto, 20))
  end

  defp add_sorting_params(params, selecto) do
    case get_current_sort(selecto) do
      nil -> params
      sort -> Map.put(params, "sort", sort)
    end
  end

  defp add_filter_params(params, _selecto) do
    # This would extract current filters from selecto
    # For now, return as-is
    params
  end

  defp get_integer_param(params, key, default) do
    case Map.get(params, key) do
      nil -> default
      "" -> default
      value when is_binary(value) ->
        case Integer.parse(value) do
          {int, _} -> int
          :error -> default
        end
      value when is_integer(value) -> value
      _ -> default
    end
  end

  defp column_type_to_input_type(:string), do: :string
  defp column_type_to_input_type(:integer), do: :number
  defp column_type_to_input_type(:decimal), do: :number
  defp column_type_to_input_type(:boolean), do: :select
  defp column_type_to_input_type(:date), do: :date
  defp column_type_to_input_type(:utc_datetime), do: :datetime
  defp column_type_to_input_type(_), do: :string

  defp get_field_options(%{type: :boolean}), do: [{"True", true}, {"False", false}]
  defp get_field_options(_), do: []

  defp humanize(atom) when is_atom(atom) do
    atom |> Atom.to_string() |> humanize()
  end
  
  defp humanize(string) when is_binary(string) do
    string
    |> String.replace("_", " ")
    |> String.split()
    |> Enum.map(&String.capitalize/1)
    |> Enum.join(" ")
  end

  # Query state tracking functions
  # These work with the selecto.set map to track current query state

  defp get_current_page(selecto, default) do
    # First check metadata, then check limit/offset to calculate page
    case get_in(selecto, [Access.key(:set, %{}), :page]) do
      nil ->
        # Calculate from limit/offset if available
        limit = Map.get(selecto.set, :limit)
        offset = Map.get(selecto.set, :offset, 0)
        if limit && limit > 0 do
          div(offset, limit) + 1
        else
          default
        end
      page -> page
    end
  end

  defp get_per_page(selecto, default) do
    # Check set for per_page or limit
    case get_in(selecto, [Access.key(:set, %{}), :per_page]) do
      nil ->
        Map.get(selecto.set, :limit, default)
      per_page -> per_page
    end
  end

  defp get_current_sort(selecto) do
    # Extract current sort from order_by in the set
    case Map.get(selecto.set, :order_by, []) do
      [] -> nil
      [{field, _direction} | _] -> field
      [field | _] when is_binary(field) -> field
      _ -> nil
    end
  end

  defp get_current_filter_value(selecto, field) do
    # Look up filter value from filtered list
    filters = Map.get(selecto.set, :filtered, [])

    Enum.find_value(filters, fn
      {^field, value} -> value
      {filter_field, value} when is_binary(filter_field) ->
        if filter_field == to_string(field), do: value, else: nil
      _ -> nil
    end)
  end

  defp put_pagination(selecto, :page, page) do
    per_page = get_per_page(selecto, 20)
    offset = (page - 1) * per_page

    selecto
    |> put_in([Access.key(:set), :page], page)
    |> put_in([Access.key(:set), :offset], offset)
    |> put_in([Access.key(:set), :limit], per_page)
  end

  defp put_pagination(selecto, :per_page, per_page) do
    page = get_current_page(selecto, 1)
    offset = (page - 1) * per_page

    selecto
    |> put_in([Access.key(:set), :per_page], per_page)
    |> put_in([Access.key(:set), :limit], per_page)
    |> put_in([Access.key(:set), :offset], offset)
  end

  defp remove_filter(selecto, field) do
    # Remove a filter from the filtered list
    field_str = to_string(field)

    updated_filters = selecto.set.filtered
    |> Enum.reject(fn
      {f, _} when is_atom(f) -> to_string(f) == field_str
      {f, _} when is_binary(f) -> f == field_str
      _ -> false
    end)

    put_in(selecto, [Access.key(:set), :filtered], updated_filters)
  end

  @doc """
  Get all current filter values as a map.

  Useful for pre-populating filter forms or debugging.

  ## Examples

      current_filters = Selecto.PhoenixHelpers.get_filters(selecto)
      # => %{"status" => "active", "category_id" => 5}
  """
  def get_filters(selecto) do
    filters = Map.get(selecto.set, :filtered, [])

    filters
    |> Enum.map(fn
      {field, value} -> {to_string(field), value}
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.into(%{})
  end

  @doc """
  Get current sort configuration.

  Returns the current sort field and direction.

  ## Examples

      {field, direction} = Selecto.PhoenixHelpers.get_sort(selecto)
      # => {"created_at", :desc}
  """
  def get_sort(selecto) do
    case Map.get(selecto.set, :order_by, []) do
      [] -> {nil, :asc}
      [{field, direction} | _] -> {field, direction}
      [field | _] when is_binary(field) -> {field, :asc}
      [{:desc, field} | _] -> {field, :desc}
      [{:asc, field} | _] -> {field, :asc}
      _ -> {nil, :asc}
    end
  end

  @doc """
  Check if a field is currently being filtered.

  ## Examples

      if Selecto.PhoenixHelpers.is_filtered?(selecto, "status") do
        # Show clear filter button
      end
  """
  def is_filtered?(selecto, field) do
    get_current_filter_value(selecto, field) != nil
  end

  @doc """
  Check if a field is currently being sorted.

  ## Examples

      {is_sorted, direction} = Selecto.PhoenixHelpers.is_sorted?(selecto, "name")
      # => {true, :asc}
  """
  def is_sorted?(selecto, field) do
    {sort_field, direction} = get_sort(selecto)
    if to_string(sort_field) == to_string(field) do
      {true, direction}
    else
      {false, :asc}
    end
  end

  @doc """
  Toggle sort direction for a field.

  If the field is not currently sorted, sorts ascending.
  If sorted ascending, switches to descending.
  If sorted descending, removes sorting.

  ## Examples

      selecto = Selecto.PhoenixHelpers.toggle_sort(selecto, "name")
  """
  def toggle_sort(selecto, field) do
    {is_sorted, current_direction} = is_sorted?(selecto, field)

    case {is_sorted, current_direction} do
      {false, _} ->
        # Not sorted - add ascending sort
        put_in(selecto, [Access.key(:set), :order_by], [{field, :asc}])

      {true, :asc} ->
        # Currently ascending - switch to descending
        put_in(selecto, [Access.key(:set), :order_by], [{field, :desc}])

      {true, :desc} ->
        # Currently descending - remove sort
        put_in(selecto, [Access.key(:set), :order_by], [])
    end
  end

  @doc """
  Clear all filters from the query.

  ## Examples

      selecto = Selecto.PhoenixHelpers.clear_filters(selecto)
  """
  def clear_filters(selecto) do
    put_in(selecto, [Access.key(:set), :filtered], [])
  end

  @doc """
  Reset query to default state.

  Clears filters, sorting, and pagination.

  ## Examples

      selecto = Selecto.PhoenixHelpers.reset_query(selecto)
  """
  def reset_query(selecto) do
    selecto
    |> clear_filters()
    |> put_in([Access.key(:set), :order_by], [])
    |> put_in([Access.key(:set), :page], 1)
    |> put_in([Access.key(:set), :offset], 0)
  end
end