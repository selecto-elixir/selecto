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
      :id,                 # Unique identifier for the subfilter
      :relationship_path,  # Parsed relationship path information
      :filter_spec,        # Filter specification (value, operator, etc.)
      :strategy,           # :exists, :in, :any, :all
      :negate,             # boolean - whether to negate the condition
      :opts                # additional options
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
      :path_segments,      # ["film"] or ["film", "category"]
      :target_table,       # final table in the path
      :target_field,       # field name on target table (optional for aggregations)
      :is_aggregation      # boolean - whether this is an aggregation subfilter
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
      :type,       # :equality, :comparison, :in_list, :range, :aggregation, :temporal
      :operator,   # SQL operator string
      :value,      # single value
      :values,     # multiple values for IN lists
      :min_value,  # for range filters
      :max_value,  # for range filters
      :agg_function, # :count, :sum, :avg, :min, :max
      :temporal_type # :recent_years, :within_days
    ]

    @type filter_type :: :equality | :comparison | :in_list | :range | :aggregation | :temporal
    @type agg_function :: :count | :sum | :avg | :min | :max
    @type temporal_type :: :recent_years | :within_days

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
      :type,        # :and, :or
      :subfilters   # list of Spec structs
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
