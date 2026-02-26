defmodule Selecto.Extension do
  @moduledoc """
  Behavior contract for Selecto ecosystem extensions.

  Extensions are loaded from domain definitions through the `:extensions` key.
  A single extension can participate in multiple packages (`selecto`,
  `selecto_components`, `selecto_updato`) by implementing the callbacks relevant
  to that package.

  ## Domain Usage

      %{
        name: "Store",
        source: %{...},
        schemas: %{},
        joins: %{},
        extensions: [
          # From the :selecto_postgis package
          Selecto.Extensions.PostGIS,
          {MyApp.Selecto.Extensions.Timescale, hypertable: "events"}
        ]
      }
  """

  @type extension_opts :: keyword()
  @type extension_spec :: module() | {module(), extension_opts()}

  @doc """
  Merge extension-provided defaults into a domain during `Selecto.configure/3`.
  """
  @callback merge_domain(domain :: map(), opts :: extension_opts()) :: map()

  @doc """
  Return DSL modules imported by `Selecto.Config.OverlayDSL` when this extension
  is enabled in an overlay module.
  """
  @callback overlay_dsl_modules(opts :: extension_opts()) :: [module()]

  @doc """
  Compile-time hook called from `Selecto.Config.OverlayDSL.__using__/1`.

  Use this to register extension-specific module attributes for overlay DSL
  directives.
  """
  @callback overlay_setup(overlay_module :: module(), opts :: extension_opts()) :: :ok

  @doc """
  Build extension-owned overlay fragments during `@before_compile`.
  """
  @callback overlay_fragment(overlay_module :: module(), opts :: extension_opts()) :: map()

  @doc """
  Return additional `selecto_components` view tuples for this extension.
  """
  @callback components_views(selecto_or_domain :: map(), opts :: extension_opts()) :: [tuple()]

  @doc """
  Merge extension defaults into a domain used by `selecto_updato`.
  """
  @callback updato_domain(domain :: map(), opts :: extension_opts()) :: map()

  @optional_callbacks merge_domain: 2,
                      overlay_dsl_modules: 1,
                      overlay_setup: 2,
                      overlay_fragment: 2,
                      components_views: 2,
                      updato_domain: 2
end
