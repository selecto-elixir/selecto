defmodule Selecto.Subfilter do
  @moduledoc """
  Core subfilter data structures and specifications.

  The Subfilter system enables filtering on related data without explicit joins
  by automatically generating subqueries (EXISTS, IN, ANY, ALL) based on
  relationship paths defined in domain configurations.

  ## Examples

      # Find actors who appeared in R-rated films
      selecto |> Selecto.subfilter("film.rating", "R")

      # Find actors with more than 5 films
      selecto |> Selecto.subfilter("film", {:count, ">", 5})

      # Multi-level relationships
      selecto |> Selecto.subfilter("film.category.name", "Action")
  """

  defmodule Spec do
    @moduledoc """
    Specification for a single subfilter operation.
    """
    defstruct [
      # Unique identifier for the subfilter
      :id,
      # Parsed relationship path information
      :relationship_path,
      # Filter specification (value, operator, etc.)
      :filter_spec,
      # :exists, :in, :any, :all
      :strategy,
      # boolean - whether to negate the condition
      :negate,
      # additional options
      :opts
    ]

    @type strategy :: :exists | :in | :any | :all

    @type t :: %__MODULE__{
            relationship_path: RelationshipPath.t(),
            filter_spec: FilterSpec.t(),
            strategy: strategy(),
            negate: boolean(),
            opts: keyword()
          }
  end

  defmodule RelationshipPath do
    @moduledoc """
    Parsed relationship path information.
    """
    defstruct [
      # ["film"] or ["film", "category"]
      :path_segments,
      # final table in the path
      :target_table,
      # field name on target table (optional for aggregations)
      :target_field,
      # boolean - whether this is an aggregation subfilter
      :is_aggregation
    ]

    @type t :: %__MODULE__{
            path_segments: [String.t()],
            target_table: String.t(),
            target_field: String.t() | nil,
            is_aggregation: boolean()
          }
  end

  defmodule FilterSpec do
    @moduledoc """
    Filter specification for subfilter conditions.
    """
    defstruct [
      # :equality, :comparison, :in_list, :range, :aggregation, :temporal
      :type,
      # SQL operator string
      :operator,
      # single value
      :value,
      # multiple values for IN lists
      :values,
      # for range filters
      :min_value,
      # for range filters
      :max_value,
      # :count, :sum, :avg, :min, :max
      :agg_function,
      # :recent_years, :within_days, :within_hours, :since_date
      :temporal_type
    ]

    @type filter_type :: :equality | :comparison | :in_list | :range | :aggregation | :temporal
    @type agg_function :: :count | :sum | :avg | :min | :max
    @type temporal_type :: :recent_years | :within_days | :within_hours | :since_date

    @type t :: %__MODULE__{
            type: filter_type(),
            operator: String.t() | nil,
            value: any(),
            values: [any()] | nil,
            min_value: any() | nil,
            max_value: any() | nil,
            agg_function: agg_function() | nil,
            temporal_type: temporal_type() | nil
          }
  end

  defmodule CompoundSpec do
    @moduledoc """
    Specification for compound subfilter operations (AND/OR).
    """
    defstruct [
      # :and, :or
      :type,
      # list of Spec structs
      :subfilters
    ]

    @type compound_type :: :and | :or

    @type t :: %__MODULE__{
            type: compound_type(),
            subfilters: [Spec.t()]
          }
  end

  defmodule Error do
    @moduledoc """
    Subfilter-specific error structure.
    """
    defexception [:message, :type, :details]

    @type error_type :: :invalid_relationship_path | :join_path_not_found | :invalid_filter_spec

    def exception(opts) do
      type = Keyword.get(opts, :type, :subfilter_error)
      message = Keyword.get(opts, :message, "Subfilter error")
      details = Keyword.get(opts, :details, %{})

      %__MODULE__{
        type: type,
        message: message,
        details: details
      }
    end
  end
end
