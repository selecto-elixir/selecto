defmodule Selecto.Advanced.CaseExpression do
  @moduledoc """
  CASE expression support for PostgreSQL conditional logic.
  
  Provides comprehensive support for both simple and searched CASE expressions,
  enabling conditional data transformation within SELECT clauses.
  
  ## Examples
  
      # Simple CASE expression
      selecto
      |> Selecto.select([
          "film.title",
          {:case, "film.rating",
            when: [
              {"G", "General Audience"},
              {"PG", "Parental Guidance"},
              {"PG-13", "Parents Strongly Cautioned"},
              {"R", "Restricted"}
            ],
            else: "Not Rated",
            as: "rating_description"
          }
        ])
      
      # Searched CASE expression
      selecto
      |> Selecto.select([
          "customer.first_name",
          {:case_when, [
              {[{"payment_total", {:>, 100}}], "Premium"},
              {[{"payment_total", {:between, 50, 100}}], "Standard"},
              {[{"payment_total", {:>, 0}}], "Basic"}
            ],
            else: "No Purchases",
            as: "customer_tier"
          }
        ])
  """
  
  defmodule Spec do
    @moduledoc """
    Specification for CASE expression definitions.
    """
    defstruct [
      :id,                    # Unique identifier for the CASE expression
      :type,                  # :simple or :searched
      :column,                # Column for simple CASE expressions
      :when_clauses,          # List of {condition, result} tuples
      :else_clause,           # Optional else result
      :alias,                 # Optional alias for the expression
      :validated              # Boolean indicating if CASE has been validated
    ]
    
    @type case_type :: :simple | :searched
    
    @type when_clause :: {any(), any()} | {[{String.t(), any()}], any()}
    
    @type t :: %__MODULE__{
      id: String.t(),
      type: case_type(),
      column: String.t() | nil,
      when_clauses: [when_clause()],
      else_clause: any() | nil,
      alias: String.t() | nil,
      validated: boolean()
    }
  end
  
  defmodule ValidationError do
    @moduledoc """
    Error raised when CASE expression specification is invalid.
    """
    defexception [:type, :message, :details]
    
    @type t :: %__MODULE__{
      type: :invalid_structure | :invalid_when_clauses | :missing_column,
      message: String.t(),
      details: map()
    }
  end
  
  @doc """
  Create a simple CASE expression specification.
  
  ## Parameters
  
  - `column` - Column to test against
  - `when_clauses` - List of {value, result} tuples
  - `opts` - Options including :else, :as
  
  ## Examples
  
      # Simple CASE with alias
      CaseExpression.create_simple_case("film.rating", [
        {"G", "General Audience"},
        {"PG", "Parental Guidance"},
        {"R", "Restricted"}
      ], else: "Not Rated", as: "rating_description")
  """
  def create_simple_case(column, when_clauses, opts \\ []) do
    spec = %Spec{
      id: generate_case_id("simple"),
      type: :simple,
      column: column,
      when_clauses: when_clauses,
      else_clause: Keyword.get(opts, :else),
      alias: Keyword.get(opts, :as),
      validated: false
    }
    
    case validate_case(spec) do
      {:ok, validated_spec} -> validated_spec
      {:error, validation_error} -> raise validation_error
    end
  end
  
  @doc """
  Create a searched CASE expression specification.
  
  ## Parameters
  
  - `when_clauses` - List of {conditions, result} tuples
  - `opts` - Options including :else, :as
  
  ## Examples
  
      # Searched CASE with multiple conditions
      CaseExpression.create_searched_case([
        {[{"payment_total", {:>, 100}}], "Premium"},
        {[{"payment_total", {:between, 50, 100}}], "Standard"},
        {[{"payment_total", {:>, 0}}], "Basic"}
      ], else: "No Purchases", as: "customer_tier")
  """
  def create_searched_case(when_clauses, opts \\ []) do
    spec = %Spec{
      id: generate_case_id("searched"),
      type: :searched,
      column: nil,
      when_clauses: when_clauses,
      else_clause: Keyword.get(opts, :else),
      alias: Keyword.get(opts, :as),
      validated: false
    }
    
    case validate_case(spec) do
      {:ok, validated_spec} -> validated_spec
      {:error, validation_error} -> raise validation_error
    end
  end
  
  @doc """
  Validate a CASE expression specification.
  
  Ensures the CASE expression structure is valid and all conditions are properly formed.
  """
  def validate_case(%Spec{} = spec) do
    with :ok <- validate_case_structure(spec),
         :ok <- validate_when_clauses(spec) do
      
      validated_spec = %{spec | validated: true}
      {:ok, validated_spec}
    else
      {:error, reason} -> {:error, reason}
    end
  end
  
  # Validate CASE expression structure
  defp validate_case_structure(%Spec{type: :simple, column: nil}) do
    {:error, %ValidationError{
      type: :missing_column,
      message: "Simple CASE expression must have a column",
      details: %{}
    }}
  end
  
  defp validate_case_structure(%Spec{type: :simple, column: column}) when is_binary(column) do
    :ok
  end
  
  defp validate_case_structure(%Spec{type: :searched, column: nil}) do
    :ok
  end
  
  defp validate_case_structure(%Spec{} = spec) do
    {:error, %ValidationError{
      type: :invalid_structure,
      message: "Invalid CASE expression structure",
      details: %{type: spec.type, column: spec.column}
    }}
  end
  
  # Validate WHEN clauses
  defp validate_when_clauses(%Spec{when_clauses: when_clauses, type: :simple}) do
    if Enum.all?(when_clauses, &valid_simple_when_clause?/1) do
      :ok
    else
      {:error, %ValidationError{
        type: :invalid_when_clauses,
        message: "Simple CASE WHEN clauses must be {value, result} tuples",
        details: %{when_clauses: when_clauses}
      }}
    end
  end
  
  defp validate_when_clauses(%Spec{when_clauses: when_clauses, type: :searched}) do
    if Enum.all?(when_clauses, &valid_searched_when_clause?/1) do
      :ok
    else
      {:error, %ValidationError{
        type: :invalid_when_clauses,
        message: "Searched CASE WHEN clauses must be {conditions_list, result} tuples",
        details: %{when_clauses: when_clauses}
      }}
    end
  end
  
  # Check if simple WHEN clause is valid
  defp valid_simple_when_clause?({_value, _result}), do: true
  defp valid_simple_when_clause?(_), do: false
  
  # Check if searched WHEN clause is valid
  defp valid_searched_when_clause?({conditions, _result}) when is_list(conditions), do: true
  defp valid_searched_when_clause?(_), do: false
  
  # Generate unique ID for CASE expression
  defp generate_case_id(type) do
    unique = :erlang.unique_integer([:positive])
    "case_#{type}_#{unique}"
  end
end