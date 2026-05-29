defmodule Selecto.Domain.Constants do
  @moduledoc false

  @query_member_groups [:ctes, :values, :subqueries, :laterals, :unnests]

  @detail_action_types [:modal, :iframe_modal, :external_link, :live_component]

  @field_filter_ops [
    :eq,
    :neq,
    :not_eq,
    :gt,
    :gte,
    :lt,
    :lte,
    :like,
    :ilike,
    :contains,
    :starts_with,
    :ends_with,
    :between,
    :in,
    :not_in,
    :text_search,
    :match_against,
    :array_contains,
    :array_contained,
    :array_overlap,
    :array_eq
  ]

  @query_member_join_types [:left, :inner, :right, :full]

  defmacro __using__(_opts) do
    quote do
      @query_member_groups unquote(@query_member_groups)
      @detail_action_types unquote(@detail_action_types)
      @field_filter_ops unquote(@field_filter_ops)
      @query_member_join_types unquote(@query_member_join_types)
    end
  end

  @spec query_member_groups() :: [atom()]
  def query_member_groups, do: @query_member_groups

  @spec query_member_group?(term()) :: boolean()
  def query_member_group?(group), do: group in @query_member_groups

  @spec detail_action_types() :: [atom()]
  def detail_action_types, do: @detail_action_types

  @spec detail_action_type?(term()) :: boolean()
  def detail_action_type?(type), do: type in @detail_action_types

  @spec field_filter_ops() :: [atom()]
  def field_filter_ops, do: @field_filter_ops

  @spec field_filter_op?(term()) :: boolean()
  def field_filter_op?(op), do: op in @field_filter_ops

  @spec query_member_join_types() :: [atom()]
  def query_member_join_types, do: @query_member_join_types

  @spec query_member_join_type?(term()) :: boolean()
  def query_member_join_type?(type), do: type in @query_member_join_types
end
