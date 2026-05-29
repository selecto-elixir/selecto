defmodule Selecto.Domain.Compose do
  @moduledoc false

  alias Selecto.Domain.Diagnostics
  alias Selecto.Domain.Shared.Map, as: MapHelpers
  alias Selecto.Domain.Shorthand

  @map_sections [
    :source,
    :schemas,
    :joins,
    :filters,
    :functions,
    :query_members,
    :published_views,
    :detail_actions,
    :columns,
    :custom_columns,
    :jsonb_schemas,
    :subfilters,
    :window_functions,
    :pagination,
    :retarget,
    :writes,
    :actions,
    :capabilities,
    :source_relationships,
    :choice_sources
  ]
  @known_sections [
    :schema_version,
    :domain_version,
    :domain_fingerprint,
    :name | @map_sections ++ [
      :default_selected,
      :required_selected,
      :required_filters,
      :required_order_by,
      :required_group_by,
      :redact_fields,
      :extensions
    ]
  ]
  @collision_warning_sections [
    :actions,
    :capabilities,
    :source_relationships,
    :choice_sources
  ]

  @spec compose(map(), list(), (map() -> {:ok, map(), map()} | {:error, map()})) ::
          {:ok, map(), map()} | {:error, map()}
  def compose(domain, overlays, normalize_fun) do
    with {:ok, normalized, _diagnostics} <- normalize_fun.(domain),
         {:ok, overlays} <- domain_overlays(overlays) do
      {composed_domain, composition_warnings} =
        overlays
        |> Enum.with_index()
        |> Enum.reduce({normalized.domain, []}, fn {overlay, index}, {acc, warnings} ->
          overlay = Shorthand.normalize_authoring_shorthand(overlay)

          {
            merge_domain_maps(acc, overlay),
            warnings ++ composition_collision_warnings(acc, overlay, index)
          }
        end)

      composed_domain = apply_composed_extensions(composed_domain)

      with {:ok, normalized, diagnostics} <- normalize_fun.(composed_domain) do
        {:ok, normalized, %{diagnostics | warnings: composition_warnings ++ diagnostics.warnings}}
      end
    end
  end

  def domain_overlays(nil), do: {:ok, []}
  def domain_overlays([]), do: {:ok, []}
  def domain_overlays(%{} = overlay), do: {:ok, [overlay]}

  def domain_overlays(overlays) when is_list(overlays) do
    overlays
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {overlay, index}, {:ok, acc} ->
      if is_map(overlay) do
        {:cont, {:ok, [overlay | acc]}}
      else
        {:halt, {:error, invalid_domain_overlay_diagnostics(overlay, index)}}
      end
    end)
    |> case do
      {:ok, overlays} -> {:ok, Enum.reverse(overlays)}
      error -> error
    end
  end

  def domain_overlays(overlay), do: {:error, invalid_domain_overlay_diagnostics(overlay, nil)}
  def invalid_domain_overlay_diagnostics(overlay, index) do
    Diagnostics.new(
      errors: [
        %{
          code: :invalid_domain_overlay,
          message: "Selecto domain overlays must be maps",
          overlay_index: index,
          actual: MapHelpers.value_type(overlay)
        }
      ]
    )
  end
  def apply_composed_extensions(domain) do
    extension_specs =
      domain
      |> MapHelpers.section(:extensions, [])
      |> Selecto.Extensions.normalize_specs()

    domain
    |> Selecto.Extensions.merge_domain_extensions(extension_specs)
    |> Shorthand.normalize_authoring_shorthand()
  end
  def merge_domain_maps(base, overlay) when is_map(base) and is_map(overlay) do
    Enum.reduce(overlay, base, fn {key, overlay_value}, acc ->
      section_key = canonical_section_key(key)
      base_value = fetch_merge_value(acc, section_key)

      merged_value =
        merge_section_value(section_key, base_value, overlay_value, [section_key])

      put_merge_value(acc, section_key, merged_value)
    end)
  end
  def merge_section_value(:extensions, base, overlay, _path)
       when is_list(base) and is_list(overlay),
       do: unique_list(base ++ overlay)

  def merge_section_value(:redact_fields, base, overlay, _path)
       when is_list(base) and is_list(overlay),
       do: unique_list(base ++ overlay)

  def merge_section_value(:source, base, overlay, path) when is_map(base) and is_map(overlay) do
    deep_merge_domain_maps(base, overlay, path)
  end

  def merge_section_value(section, base, overlay, path)
       when section in @collision_warning_sections and is_map(base) and is_map(overlay) do
    merge_registry_maps(base, overlay, path)
  end

  def merge_section_value(section, base, overlay, path)
       when section in @map_sections and is_map(base) and is_map(overlay) do
    deep_merge_domain_maps(base, overlay, path)
  end

  def merge_section_value(_section, _base, overlay, _path), do: overlay
  def deep_merge_domain_maps(base, overlay, path) do
    Map.merge(base, overlay, fn key, base_value, overlay_value ->
      merge_nested_value(path ++ [canonical_section_key(key)], base_value, overlay_value)
    end)
  end
  def merge_nested_value([:source, :redact_fields], base, overlay)
       when is_list(base) and is_list(overlay),
       do: unique_list(base ++ overlay)

  def merge_nested_value(_path, base, overlay) when is_map(base) and is_map(overlay) do
    deep_merge_domain_maps(base, overlay, [])
  end

  def merge_nested_value(_path, _base, overlay), do: overlay
  def merge_registry_maps(base, overlay, path) do
    Enum.reduce(overlay, base, fn {overlay_key, overlay_value}, acc ->
      case equivalent_map_key(acc, overlay_key) do
        nil ->
          Map.put(acc, overlay_key, overlay_value)

        base_key ->
          merged_value =
            merge_nested_value(path ++ [base_key], Map.fetch!(acc, base_key), overlay_value)

          Map.put(acc, base_key, merged_value)
      end
    end)
  end
  def equivalent_map_key(map, key) do
    Enum.find(Map.keys(map), &(merge_key_id(&1) == merge_key_id(key)))
  end
  def composition_collision_warnings(base, overlay, overlay_index) do
    Enum.flat_map(@collision_warning_sections, fn section ->
      base_section = MapHelpers.section(base, section, %{})
      overlay_section = MapHelpers.section(overlay, section, %{})

      if is_map(base_section) and is_map(overlay_section) do
        base_keys = Map.keys(base_section)

        overlay_section
        |> Map.keys()
        |> Enum.filter(&merge_key_member?(base_keys, &1))
        |> Enum.map(&composition_collision_warning(section, &1, overlay_index))
      else
        []
      end
    end)
  end
  def composition_collision_warning(section, key, overlay_index) do
    %{
      code: :domain_composition_collision,
      message:
        "domain overlay #{overlay_index} updates existing #{inspect(section)} entry #{inspect(key)}",
      section: section,
      key: key,
      overlay_index: overlay_index
    }
  end
  def merge_key_member?(keys, key) do
    Enum.any?(keys, &(merge_key_id(&1) == merge_key_id(key)))
  end
  def merge_key_id(key) when is_atom(key), do: Atom.to_string(key)
  def merge_key_id(key) when is_binary(key), do: key
  def merge_key_id(key), do: inspect(key)
  def fetch_merge_value(map, key) when is_atom(key), do: MapHelpers.section(map, key)
  def fetch_merge_value(map, key), do: Map.get(map, key)
  def put_merge_value(map, key, value) when is_atom(key), do: MapHelpers.put_section(map, key, value)
  def put_merge_value(map, key, value), do: Map.put(map, key, value)
  def canonical_section_key(key) when is_atom(key), do: key

  def canonical_section_key(key) when is_binary(key) do
    Enum.find(@known_sections, &(Atom.to_string(&1) == key)) || key
  end

  def canonical_section_key(key), do: key
  def unique_list(values), do: Enum.uniq(values)
end
