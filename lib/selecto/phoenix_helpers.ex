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

  # These functions would need to be implemented in Selecto core
  # For now, they're placeholders that work with a custom metadata system

  defp get_current_page(selecto, default) do
    get_in(selecto, [Access.key(:metadata, %{}), :page]) || default
  end

  defp get_per_page(selecto, default) do
    get_in(selecto, [Access.key(:metadata, %{}), :per_page]) || default
  end

  defp get_current_sort(_selecto), do: nil

  defp get_current_filter_value(_selecto, _field), do: nil

  defp put_pagination(selecto, key, value) do
    metadata = Map.get(selecto, :metadata, %{})
    updated_metadata = Map.put(metadata, key, value)
    Map.put(selecto, :metadata, updated_metadata)
  end

  defp remove_filter(selecto, _field) do
    # This would need to be implemented in Selecto core
    selecto
  end
end