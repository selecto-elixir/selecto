defmodule Listable.Components do

  use Phoenix.Component

  def view_panel(assigns) do
    ~H"""
      <div>
      View <%= @listable.repo %>
      </div>
    """
  end

  def filter_panel(assigns) do
    ~H"""
      <div>
      Filter <%= @listable.repo %>
      </div>
    """
  end

  def results_panel(assigns) do
    ~H"""
      <div>
      Results <%= @listable.repo %>
      </div>
    """
  end

end
