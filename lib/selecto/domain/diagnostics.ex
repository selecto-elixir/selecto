defmodule Selecto.Domain.Diagnostics do
  @moduledoc """
  Structured diagnostics returned by `Selecto.Domain.normalize/1`.

  Diagnostics are intentionally non-fatal for recognized compatibility cases.
  They let callers inspect inferred versions, projection/proposed sections, and
  unknown top-level keys before any runtime behavior moves to the normalized
  contract.
  """

  alias Selecto.Domain.Sections

  defstruct errors: [],
            warnings: [],
            sections: Sections.empty_sections(),
            canonical_sections: [],
            projection_sections: [],
            proposed_sections: [],
            unknown_sections: [],
            schema_version: nil,
            schema_version_inferred: false

  @type warning :: %{
          required(:code) => atom(),
          required(:message) => String.t(),
          optional(:sections) => [term()],
          optional(:schema_version) => pos_integer()
        }

  @type t :: %__MODULE__{
          errors: [map()],
          warnings: [warning()],
          sections: map(),
          canonical_sections: [term()],
          projection_sections: [term()],
          proposed_sections: [term()],
          unknown_sections: [term()],
          schema_version: pos_integer() | term() | nil,
          schema_version_inferred: boolean()
        }

  @doc """
  Builds a diagnostics struct.
  """
  @spec new(keyword()) :: t()
  def new(attrs \\ []) do
    sections = Keyword.get(attrs, :sections, Sections.empty_sections())
    schema_version = Keyword.get(attrs, :schema_version)
    schema_version_inferred = Keyword.get(attrs, :schema_version_inferred, false)
    warnings = Keyword.get(attrs, :warnings, [])
    errors = Keyword.get(attrs, :errors, [])

    %__MODULE__{
      errors: errors,
      warnings: warnings ++ section_warnings(sections, schema_version, schema_version_inferred),
      sections: sections,
      canonical_sections: Map.get(sections, :canonical, []),
      projection_sections: Map.get(sections, :projection, []),
      proposed_sections: Map.get(sections, :proposed, []),
      unknown_sections: Map.get(sections, :unknown, []),
      schema_version: schema_version,
      schema_version_inferred: schema_version_inferred
    }
  end

  defp section_warnings(sections, schema_version, schema_version_inferred) do
    []
    |> maybe_add(
      schema_version_inferred,
      %{
        code: :schema_version_inferred,
        message: "schema_version was not present and was inferred for compatibility",
        schema_version: schema_version
      }
    )
    |> maybe_add(
      Map.get(sections, :projection, []) != [],
      %{
        code: :projection_sections,
        message: "projection or implementation-facing domain sections are present",
        sections: Map.get(sections, :projection, [])
      }
    )
    |> maybe_add(
      Map.get(sections, :proposed, []) != [],
      %{
        code: :proposed_sections,
        message: "proposed future canonical domain sections are present",
        sections: Map.get(sections, :proposed, [])
      }
    )
    |> maybe_add(
      Map.get(sections, :unknown, []) != [],
      %{
        code: :unknown_sections,
        message: "unknown top-level domain sections are present",
        sections: Map.get(sections, :unknown, [])
      }
    )
    |> Enum.reverse()
  end

  defp maybe_add(warnings, true, warning), do: [warning | warnings]
  defp maybe_add(warnings, false, _warning), do: warnings
end
