defmodule Listable.Components do
  use Phoenix.Component
  use PetalComponents

  def view_panel(assigns) do
    ~H"""
      <.accordion>
        <:item heading="View Options">
          VIEW OPTS
        </:item>
        <:item heading="Filter Options">
          Filter OPTS
        </:item>
        <:item heading="Export Options">
          Export OPTS
        </:item>
      </.accordion>

    """
  end


  def results_panel(assigns) do
    results = Listable.execute(assigns.listable)
    assigns = assign(assigns, results: results)



    ~H"""
      <div>
        Results TODO MAKE FANCY TABLE
          <div :for={r <- @results}>
            <%= inspect(r) %>
          </div>
      </div>
    """
  end
end
