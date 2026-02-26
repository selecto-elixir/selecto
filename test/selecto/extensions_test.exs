defmodule Selecto.ExtensionsTest do
  use ExUnit.Case, async: true

  defmodule MarkerExtension do
    @behaviour Selecto.Extension

    @impl true
    def merge_domain(domain, _opts) do
      Map.put(domain, :marker_extension_applied, true)
    end

    @impl true
    def updato_domain(domain, _opts) do
      Map.put(domain, :marker_updato_applied, true)
    end
  end

  defmodule EctoTypeMarkerExtension do
    @behaviour Selecto.Extension

    defmodule GeoCustomType do
      @behaviour Ecto.Type

      def type, do: :string
      def cast(term), do: {:ok, term}
      def load(term), do: {:ok, term}
      def dump(term), do: {:ok, term}
      def embed_as(_format), do: :self
      def equal?(left, right), do: left == right
    end

    @impl true
    def ecto_type_to_selecto_type(__MODULE__.GeoCustomType, _opts), do: :geometry
    def ecto_type_to_selecto_type(_, _opts), do: nil
  end

  defmodule OverlayMarkerExtension do
    @behaviour Selecto.Extension

    @impl true
    def overlay_dsl_modules(_opts), do: [__MODULE__.OverlayDSL]

    @impl true
    def overlay_setup(overlay_module, _opts) do
      Module.register_attribute(overlay_module, :overlay_marker, accumulate: false)
      :ok
    end

    @impl true
    def overlay_fragment(overlay_module, _opts) do
      case Module.get_attribute(overlay_module, :overlay_marker) do
        %{} = marker -> %{marker: marker}
        _ -> %{}
      end
    end

    @impl true
    def components_views(_selecto_or_domain, _opts) do
      [{:marker, __MODULE__.MarkerView, "Marker View", %{drill_down: :detail}}]
    end

    defmodule OverlayDSL do
      defmacro defmarker(value) do
        quote do
          @overlay_marker %{value: unquote(value)}
        end
      end
    end

    defmodule MarkerView do
    end
  end

  defmodule MarkerOverlay do
    use Selecto.Config.OverlayDSL,
      extensions: [OverlayMarkerExtension]

    defmarker("ok")
  end

  defmodule EctoAdapterGeoSchema do
    use Ecto.Schema

    schema "geo_items" do
      field(:name, :string)
      field(:location, EctoTypeMarkerExtension.GeoCustomType)
    end
  end

  test "normalizes extension specs" do
    assert Selecto.Extensions.normalize_specs(nil) == []

    normalized =
      Selecto.Extensions.normalize_specs([
        MarkerExtension,
        {MarkerExtension, enabled: true},
        %{module: MarkerExtension, opts: %{source: :test}}
      ])

    assert {MarkerExtension, []} in normalized
    assert {MarkerExtension, [enabled: true]} in normalized
    assert {MarkerExtension, [source: :test]} in normalized
  end

  test "configure applies extension merge_domain callbacks" do
    domain =
      base_domain()
      |> Map.put(:extensions, [MarkerExtension])

    selecto = Selecto.configure(domain, nil)

    assert selecto.domain.marker_extension_applied == true
    assert selecto.extensions == [{MarkerExtension, []}]
    assert selecto.config.extensions == [{MarkerExtension, []}]
  end

  test "overlay extension contributes custom overlay fragment" do
    overlay = MarkerOverlay.overlay()

    assert overlay.marker.value == "ok"
  end

  test "components view callbacks are dispatched from extension specs" do
    views = Selecto.Extensions.components_views(base_domain(), [{OverlayMarkerExtension, []}])

    assert [{:marker, OverlayMarkerExtension.MarkerView, "Marker View", %{drill_down: :detail}}] =
             views
  end

  test "ecto type callbacks are dispatched from extension specs" do
    mapped =
      Selecto.Extensions.ecto_type_to_selecto_type(
        EctoTypeMarkerExtension.GeoCustomType,
        [{EctoTypeMarkerExtension, []}]
      )

    assert mapped == :geometry
  end

  test "ecto adapter uses extension ecto type mappings" do
    domain =
      Selecto.EctoAdapter.schema_to_domain(
        EctoAdapterGeoSchema,
        extensions: [EctoTypeMarkerExtension]
      )

    assert domain.source.columns.location.type == :geometry
    assert domain.source.columns.name.type == :string
  end

  defp base_domain do
    %{
      name: "Store",
      source: %{
        source_table: "stores",
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end
end
