defmodule Selecto.OptionsValidator do
  @moduledoc false

  @configure_option_keys [:validate, :pool, :pool_options, :adapter, :rollup_sort_fix]
  @configure_schema [
    validate: [type: :boolean],
    pool: [type: :boolean],
    pool_options: [type: :keyword_list],
    adapter: [type: :atom],
    rollup_sort_fix: [type: {:in, [true, false, :auto]}]
  ]

  @execute_option_keys [
    :timeout,
    :log,
    :max_rows,
    :receive_timeout,
    :queue_timeout,
    :stream_timeout,
    :analyze_complexity,
    :format,
    :format_options,
    :cache,
    :cache_ttl,
    :cache_namespace,
    :stream_producer
  ]

  @execute_schema [
    timeout: [type: {:or, [:non_neg_integer, {:in, [nil]}]}],
    log: [type: :boolean],
    max_rows: [type: :pos_integer],
    receive_timeout: [type: {:or, [:non_neg_integer, {:in, [:infinity]}]}],
    queue_timeout: [type: {:or, [:non_neg_integer, {:in, [:infinity]}]}],
    stream_timeout: [type: {:or, [:non_neg_integer, {:in, [:infinity]}]}],
    analyze_complexity: [type: :boolean],
    format: [type: :any],
    format_options: [type: :any],
    cache: [type: :boolean],
    cache_ttl: [type: {:or, [:pos_integer, {:in, [nil]}]}],
    cache_namespace: [type: {:or, [:string, {:in, [nil]}]}],
    stream_producer: [type: {:fun, 1}]
  ]

  @to_sql_option_keys [:pretty, :highlight, :indent, :include_comments, :pretty_print]
  @to_sql_schema [
    pretty: [type: :boolean],
    highlight: [type: {:in, [nil, false, :ansi, :markdown]}],
    indent: [type: :string],
    include_comments: [type: :boolean],
    pretty_print: [type: :boolean]
  ]

  @diagnostic_option_keys [
    :analyze,
    :verbose,
    :buffers,
    :settings,
    :wal,
    :timing,
    :costs,
    :summary,
    :format,
    :to_sql_opts
  ]

  @diagnostic_schema [
    analyze: [type: :boolean],
    verbose: [type: :boolean],
    buffers: [type: :boolean],
    settings: [type: :boolean],
    wal: [type: :boolean],
    timing: [type: :boolean],
    costs: [type: :boolean],
    summary: [type: :boolean],
    format: [type: {:in, [nil, :text, :json, :yaml, :xml]}],
    to_sql_opts: [type: :keyword_list]
  ]

  @spec validate_configure_opts!(keyword()) :: :ok
  def validate_configure_opts!(opts) when is_list(opts) do
    validate_known_options!(opts, @configure_option_keys, @configure_schema)
  end

  @spec validate_execute_opts!(keyword()) :: :ok
  def validate_execute_opts!(opts) when is_list(opts) do
    validate_known_options!(opts, @execute_option_keys, @execute_schema)
  end

  @spec validate_to_sql_opts!(keyword()) :: :ok
  def validate_to_sql_opts!(opts) when is_list(opts) do
    validate_known_options!(opts, @to_sql_option_keys, @to_sql_schema)
  end

  @spec validate_diagnostic_opts!(keyword()) :: :ok
  def validate_diagnostic_opts!(opts) when is_list(opts) do
    validate_known_options!(opts, @diagnostic_option_keys, @diagnostic_schema)
  end

  @spec validate_known_options!(keyword(), [atom()], keyword()) :: :ok
  def validate_known_options!(opts, keys, schema)
      when is_list(opts) and is_list(keys) and is_list(schema) do
    opts
    |> Keyword.take(keys)
    |> NimbleOptions.validate!(schema)

    :ok
  end
end
