defmodule Listable.Components do

  use Phoenix.Component

  def view_panel(assigns) do
    ~H"""
      <div>
      HERE <%= @listable.repo %>
      </div>
    """
  end

end
