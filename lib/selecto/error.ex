defmodule Selecto.Error do
  @moduledoc """
  Standardized error structure for all Selecto operations.

  Provides consistent error handling across the Selecto ecosystem with
  structured error information including context, query details, and
  actionable error types.

  ## Error Types

  - `:connection_error` - Database connection failures
  - `:query_error` - SQL query execution failures
  - `:validation_error` - Input validation failures
  - `:configuration_error` - Invalid domain or Selecto configuration
  - `:no_results` - Query returned no results when one expected
  - `:multiple_results` - Query returned multiple results when one expected
  - `:timeout_error` - Query execution timeout

  ## Examples

      # Connection error
      {:error, %Selecto.Error{
        type: :connection_error,
        message: "Failed to connect to database",
        details: %{host: "localhost", port: 5432}
      }}

      # Query error with context
      {:error, %Selecto.Error{
        type: :query_error,
        message: "Column 'invalid_col' does not exist",
        query: "SELECT invalid_col FROM users",
        params: [],
        details: %{column: "invalid_col", table: "users"}
      }}
  """

  defstruct [:type, :message, :details, :query, :params]

  @type t :: %__MODULE__{
    type: error_type(),
    message: String.t(),
    details: map() | nil,
    query: String.t() | nil,
    params: [term()] | nil
  }

  @type error_type ::
    :connection_error |
    :query_error |
    :validation_error |
    :configuration_error |
    :no_results |
    :multiple_results |
    :timeout_error |
    :field_resolution_error |
    :transformation_error

  @doc """
  Creates a connection error.
  """
  @spec connection_error(String.t(), map()) :: t()
  def connection_error(message, details \\ %{}) do
    %__MODULE__{
      type: :connection_error,
      message: message,
      details: details
    }
  end

  @doc """
  Creates a query execution error with SQL context.
  """
  @spec query_error(String.t(), String.t() | nil, [term()], map()) :: t()
  def query_error(message, query \\ nil, params \\ [], details \\ %{}) do
    %__MODULE__{
      type: :query_error,
      message: message,
      query: query,
      params: params,
      details: details
    }
  end

  @doc """
  Creates a validation error.
  """
  @spec validation_error(String.t(), map()) :: t()
  def validation_error(message, details \\ %{}) do
    %__MODULE__{
      type: :validation_error,
      message: message,
      details: details
    }
  end

  @doc """
  Creates a configuration error.
  """
  @spec configuration_error(String.t(), map()) :: t()
  def configuration_error(message, details \\ %{}) do
    %__MODULE__{
      type: :configuration_error,
      message: message,
      details: details
    }
  end

  @doc """
  Creates a no results error for execute_one/2.
  """
  @spec no_results_error(String.t()) :: t()
  def no_results_error(message \\ "Query returned no results") do
    %__MODULE__{
      type: :no_results,
      message: message
    }
  end

  @doc """
  Creates a multiple results error for execute_one/2.
  """
  @spec multiple_results_error(String.t()) :: t()
  def multiple_results_error(message \\ "Query returned multiple results when one expected") do
    %__MODULE__{
      type: :multiple_results,
      message: message
    }
  end

  @doc """
  Creates a timeout error.
  """
  @spec timeout_error(String.t(), map()) :: t()
  def timeout_error(message, details \\ %{}) do
    %__MODULE__{
      type: :timeout_error,
      message: message,
      details: details
    }
  end

  @doc """
  Creates a query generation error.
  """
  @spec query_generation_error(String.t(), map()) :: t()
  def query_generation_error(message, details \\ %{}) do
    %__MODULE__{
      type: :query_error,
      message: message,
      details: details
    }
  end

  @doc """
  Creates a field resolution error with context.
  """
  @spec field_resolution_error(String.t(), term(), map()) :: t()
  def field_resolution_error(message, field_ref, context \\ %{}) do
    %__MODULE__{
      type: :field_resolution_error,
      message: message,
      details: Map.merge(context, %{field_reference: field_ref})
    }
  end

  @doc """
  Creates a transformation error for output format processing.
  """
  @spec transformation_error(String.t(), map()) :: t()
  def transformation_error(message, details \\ %{}) do
    %__MODULE__{
      type: :transformation_error,
      message: message,
      details: details
    }
  end

  @doc """
  Converts various error types to standardized Selecto.Error.
  """
  @spec from_reason(term()) :: t()
  def from_reason({:exit, reason}) do
    connection_error("Database connection failed", %{reason: reason})
  end

  def from_reason(%{__exception__: true, message: message} = exception) do
    query_error(message, nil, [], %{exception: exception})
  end

  def from_reason(:no_results) do
    no_results_error()
  end

  def from_reason(:multiple_results) do
    multiple_results_error()
  end

  def from_reason(reason) when is_binary(reason) do
    query_error(reason)
  end

  def from_reason(reason) do
    query_error("Execution failed", nil, [], %{reason: reason})
  end

  @doc """
  Converts a Selecto.Error to an exception for raising.
  """
  @spec to_exception(t()) :: Exception.t()
  def to_exception(%__MODULE__{type: :connection_error, message: message}) do
    RuntimeError.exception("Database connection failed: #{message}")
  end

  def to_exception(%__MODULE__{message: message}) do
    RuntimeError.exception(message)
  end

  @doc """
  Creates a user-friendly error message for display.
  """
  @spec to_display_message(t()) :: String.t()
  def to_display_message(%__MODULE__{type: :connection_error, message: message}) do
    "Database connection failed: #{message}"
  end

  def to_display_message(%__MODULE__{type: :query_error, message: message}) do
    "Query execution failed: #{message}"
  end

  def to_display_message(%__MODULE__{type: :validation_error, message: message}) do
    "Validation error: #{message}"
  end

  def to_display_message(%__MODULE__{type: :configuration_error, message: message}) do
    "Configuration error: #{message}"
  end

  def to_display_message(%__MODULE__{type: :no_results}) do
    "No results found"
  end

  def to_display_message(%__MODULE__{type: :multiple_results}) do
    "Expected one result, but got multiple"
  end

  def to_display_message(%__MODULE__{type: :timeout_error, message: message}) do
    "Query timeout: #{message}"
  end

  def to_display_message(%__MODULE__{type: :field_resolution_error, message: message}) do
    "Field resolution error: #{message}"
  end

  def to_display_message(%__MODULE__{type: :transformation_error, message: message}) do
    "Output format transformation error: #{message}"
  end

  def to_display_message(%__MODULE__{message: message}) do
    message
  end
end
