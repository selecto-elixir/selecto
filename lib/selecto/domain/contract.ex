defmodule Selecto.Domain.Contract do
  @moduledoc """
  First-wave canonical domain contract checks.

  This module validates the normalized shape produced by `Selecto.Domain`.
  It is intentionally small: it covers the required core sections and the first
  strict subschemas for `source`, `schemas`, `joins`, and filter references.
  Existing runtime configuration does not call this module unless a caller opts
  into normalized validation.
  """

  use Selecto.Domain.Constants

  alias Selecto.Domain.Contract.Actions
  alias Selecto.Domain.Contract.Capabilities
  alias Selecto.Domain.Contract.ChoiceSources
  alias Selecto.Domain.Contract.DetailActions
  alias Selecto.Domain.Contract.FieldBindings
  alias Selecto.Domain.Contract.Joins
  alias Selecto.Domain.Contract.PublishedViews
  alias Selecto.Domain.Contract.Query
  alias Selecto.Domain.Contract.QueryMembers, as: QueryMembersValidator
  alias Selecto.Domain.Contract.Relations
  alias Selecto.Domain.Contract.Shared.Core
  alias Selecto.Domain.Contract.SourceRelationships
  alias Selecto.Domain.Contract.Writes

  @required_sections [:source, :schemas]

  @type error :: %{
          required(:code) => atom(),
          required(:message) => String.t(),
          required(:path) => [term()]
        }

  @doc """
  Returns `:ok` when a normalized domain satisfies the first-wave contract.
  """
  @spec validate(map()) :: :ok | {:error, [error()]}
  def validate(normalized_domain) when is_map(normalized_domain) do
    case errors(normalized_domain) do
      [] -> :ok
      errors -> {:error, errors}
    end
  end

  @doc """
  Returns structured contract errors for a normalized domain.
  """
  @spec errors(map()) :: [error()]
  def errors(%{authored_domain: authored_domain} = normalized_domain) do
    source = Map.get(normalized_domain, :source)
    schemas = Map.get(normalized_domain, :schemas, %{})
    joins = Map.get(normalized_domain, :joins, %{})
    query = Map.get(normalized_domain, :query, %{})
    projection = Map.get(normalized_domain, :projection, %{})
    writes = Map.get(normalized_domain, :writes, %{})
    capabilities = Map.get(normalized_domain, :capabilities, %{})
    actions = Map.get(normalized_domain, :actions, %{})
    source_relationships = Map.get(normalized_domain, :source_relationships, %{})
    choice_sources = Map.get(normalized_domain, :choice_sources, %{})
    detail_actions = Map.get(normalized_domain, :detail_actions, %{})
    field_index = Core.field_index(source, schemas, projection)

    []
    |> validate_required_sections(authored_domain)
    |> Relations.validate(source, schemas)
    |> Joins.validate(joins, source, schemas)
    |> Query.validate(query, field_index)
    |> QueryMembersValidator.validate(query)
    |> PublishedViews.validate(query)
    |> DetailActions.validate(detail_actions, field_index)
    |> Writes.validate(writes, field_index)
    |> Capabilities.validate(capabilities)
    |> Capabilities.validate_query_references(query, detail_actions, capabilities)
    |> Actions.validate(actions, capabilities, writes, field_index)
    |> SourceRelationships.validate(source_relationships, field_index)
    |> ChoiceSources.validate(choice_sources, source_relationships, capabilities)
    |> FieldBindings.validate(source, schemas, projection, choice_sources, field_index)
    |> Enum.reverse()
  end

  def errors(_normalized_domain) do
    [
      Core.error(
        :invalid_normalized_domain,
        [],
        "expected a normalized Selecto domain from Selecto.Domain.normalize/1"
      )
    ]
  end

  defp validate_required_sections(errors, authored_domain) do
    Enum.reduce(@required_sections, errors, fn section, acc ->
      if Core.has_key?(authored_domain, section) do
        acc
      else
        [
          Core.error(
            :missing_required_section,
            [section],
            "required domain section #{inspect(section)} is missing",
            section: section
          )
          | acc
        ]
      end
    end)
  end
end
