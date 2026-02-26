defmodule Selecto.Extensions do
  @moduledoc """
  Shared extension loading and callback dispatch for the Selecto ecosystem.
  """

  @typedoc "Normalized extension entries: `{module, opts}`"
  @type normalized_spec :: {module(), keyword()}

  @doc """
  Normalize extension specs from any supported format.

  Supported entries:

  - `MyExtension`
  - `{MyExtension, key: "value"}`
  - `%{module: MyExtension, opts: [...]}`
  """
  @spec normalize_specs(term()) :: [normalized_spec()]
  def normalize_specs(nil), do: []
  def normalize_specs([]), do: []

  def normalize_specs(specs) when is_list(specs) do
    Enum.map(specs, &normalize_spec!/1)
  end

  def normalize_specs(spec), do: [normalize_spec!(spec)]

  @doc """
  Extract normalized extension specs from a domain-like map.
  """
  @spec from_domain(map()) :: [normalized_spec()]
  def from_domain(%{} = domain) do
    domain
    |> Map.get(:extensions, [])
    |> normalize_specs()
  end

  def from_domain(_), do: []

  @doc """
  Extract normalized extension specs from a selecto/domain-like value.
  """
  @spec from_source(term()) :: [normalized_spec()]
  def from_source(%{extensions: extensions}) do
    normalize_specs(extensions)
  end

  def from_source(%{config: %{extensions: extensions}}) do
    normalize_specs(extensions)
  end

  def from_source(%{domain: %{} = domain}) do
    from_domain(domain)
  end

  def from_source(%{} = domain), do: from_domain(domain)
  def from_source(_), do: []

  @doc """
  Apply `merge_domain/2` callbacks for extensions in declaration order.
  """
  @spec merge_domain_extensions(map(), [normalized_spec()]) :: map()
  def merge_domain_extensions(%{} = domain, specs) when is_list(specs) do
    Enum.reduce(specs, domain, fn {module, opts}, acc ->
      maybe_invoke(module, :merge_domain, [acc, opts], acc)
    end)
  end

  def merge_domain_extensions(%{} = domain, _), do: domain

  @doc """
  Apply `updato_domain/2` callbacks for extensions in declaration order.
  """
  @spec merge_updato_domain_extensions(map(), [normalized_spec()]) :: map()
  def merge_updato_domain_extensions(%{} = domain, specs) when is_list(specs) do
    Enum.reduce(specs, domain, fn {module, opts}, acc ->
      maybe_invoke(module, :updato_domain, [acc, opts], acc)
    end)
  end

  def merge_updato_domain_extensions(%{} = domain, _), do: domain

  @doc """
  Return additional overlay DSL import modules from extension callbacks.
  """
  @spec overlay_dsl_modules([normalized_spec()]) :: [module()]
  def overlay_dsl_modules(specs) when is_list(specs) do
    specs
    |> Enum.flat_map(fn {module, opts} ->
      maybe_invoke(module, :overlay_dsl_modules, [opts], [])
      |> List.wrap()
    end)
    |> Enum.uniq()
  end

  @doc """
  Execute compile-time `overlay_setup/2` callbacks.
  """
  @spec setup_overlay_extensions(module(), [normalized_spec()]) :: :ok
  def setup_overlay_extensions(overlay_module, specs)
      when is_atom(overlay_module) and is_list(specs) do
    Enum.each(specs, fn {module, opts} ->
      _ = maybe_invoke(module, :overlay_setup, [overlay_module, opts], :ok)
    end)

    :ok
  end

  @doc """
  Build and deep-merge extension overlay fragments.
  """
  @spec overlay_fragments(module(), [normalized_spec()]) :: map()
  def overlay_fragments(overlay_module, specs) when is_atom(overlay_module) and is_list(specs) do
    Enum.reduce(specs, %{}, fn {module, opts}, acc ->
      fragment =
        module
        |> maybe_invoke(:overlay_fragment, [overlay_module, opts], %{})
        |> ensure_map()

      deep_merge(acc, fragment)
    end)
  end

  @doc """
  Return additional `selecto_components` views contributed by extensions.
  """
  @spec components_views(term(), [normalized_spec()]) :: [tuple()]
  def components_views(selecto_or_domain, specs) when is_list(specs) do
    Enum.flat_map(specs, fn {module, opts} ->
      maybe_invoke(module, :components_views, [selecto_or_domain, opts], [])
      |> List.wrap()
      |> Enum.filter(&valid_view_tuple?/1)
    end)
  end

  @doc """
  Deep-merge maps recursively.
  """
  @spec deep_merge(map(), map()) :: map()
  def deep_merge(left, right) when is_map(left) and is_map(right) do
    Map.merge(left, right, fn _key, left_val, right_val ->
      if is_map(left_val) and is_map(right_val),
        do: deep_merge(left_val, right_val),
        else: right_val
    end)
  end

  def deep_merge(left, _right), do: left

  defp normalize_spec!(module) when is_atom(module) do
    {module, []}
  end

  defp normalize_spec!({module, opts}) when is_atom(module) and is_list(opts) do
    {module, opts}
  end

  defp normalize_spec!({module, %{} = opts}) when is_atom(module) do
    {module, Map.to_list(opts)}
  end

  defp normalize_spec!(%{module: module, opts: opts}) when is_atom(module) and is_list(opts) do
    {module, opts}
  end

  defp normalize_spec!(%{module: module, opts: %{} = opts}) when is_atom(module) do
    {module, Map.to_list(opts)}
  end

  defp normalize_spec!(invalid) do
    raise ArgumentError,
          "Invalid Selecto extension spec: #{inspect(invalid)}. " <>
            "Expected module, {module, opts}, or %{module: module, opts: opts}."
  end

  defp maybe_invoke(module, callback, args, fallback)
       when is_atom(module) and is_atom(callback) and is_list(args) do
    arity = length(args)

    if Code.ensure_loaded?(module) and function_exported?(module, callback, arity) do
      apply(module, callback, args)
    else
      fallback
    end
  end

  defp maybe_invoke(_module, _callback, _args, fallback), do: fallback

  defp valid_view_tuple?({id, view_module, name, options})
       when is_atom(id) and is_atom(view_module) and is_binary(name) and is_map(options),
       do: true

  defp valid_view_tuple?(_), do: false

  defp ensure_map(%{} = map), do: map
  defp ensure_map(_), do: %{}
end
