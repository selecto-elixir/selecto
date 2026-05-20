defmodule Selecto.CapabilitiesTest do
  use ExUnit.Case, async: true

  alias Selecto.Capabilities
  alias Selecto.Capabilities.{Decision, Request}

  defmodule ModuleResolver do
    @behaviour Selecto.Capabilities.Resolver

    @impl true
    def decide(request, context) do
      Selecto.Capabilities.allow(:module_allowed,
        metadata: %{capability: request.capability, context: context}
      )
    end
  end

  defmodule BatchResolver do
    @behaviour Selecto.Capabilities.Resolver

    @impl true
    def decide(_request, _context), do: Selecto.Capabilities.deny(:should_not_call_single)

    @impl true
    def decide_many(requests, context) do
      Enum.map(requests, fn request ->
        Selecto.Capabilities.allow(:batch_allowed,
          metadata: %{capability: request.capability, context: context}
        )
      end)
    end
  end

  describe "request/1" do
    test "builds stable request structs from keyword attributes" do
      request =
        Capabilities.request(
          actor: %{id: 1},
          tenant: "tenant-a",
          domain: :orders,
          capability: "order.approve",
          operation: :execute_action,
          target: %{type: :row, id: 123},
          context: %{surface: :components},
          metadata: %{trace_id: "abc"}
        )

      assert %Request{} = request
      assert request.actor == %{id: 1}
      assert request.tenant == "tenant-a"
      assert request.domain == :orders
      assert request.capability == "order.approve"
      assert request.operation == :execute_action
      assert request.target == %{type: :row, id: 123}
      assert request.context == %{surface: :components}
      assert request.metadata == %{trace_id: "abc"}
    end

    test "accepts string-keyed request maps and requires capability and operation" do
      request =
        Request.new(%{
          "capability" => "order.view",
          "operation" => "select",
          "target" => %{"type" => "domain"}
        })

      assert request.capability == "order.view"
      assert request.operation == "select"
      assert request.target == %{"type" => "domain"}
      assert request.context == %{}

      assert_raise ArgumentError,
                   ~r/missing required capability request attribute :operation/,
                   fn ->
                     Request.new(capability: "order.view")
                   end

      assert_raise ArgumentError,
                   ~r/capability request :capability must be an atom or string/,
                   fn ->
                     Request.new(capability: 123, operation: :select)
                   end
    end
  end

  describe "decision helpers" do
    test "build allow, deny, hidden, preview-only, and not-applicable decisions" do
      assert %Decision{
               status: :allow,
               visibility: :enabled,
               reason_code: :role_allowed,
               effects: [{:required_filter, "tenant_id", {:eq, 1}}]
             } =
               Capabilities.allow(:role_allowed,
                 effects: [{:required_filter, "tenant_id", {:eq, 1}}]
               )

      assert %Decision{status: :deny, visibility: :disabled, reason_code: :missing_role} =
               Capabilities.deny(:missing_role)

      assert %Decision{status: :deny, visibility: :hidden, reason_code: :sensitive} =
               Capabilities.hidden(:sensitive)

      assert %Decision{
               status: :conditional,
               visibility: :preview_only,
               reason_code: :requires_confirmation,
               obligations: [:audit_action]
             } =
               Capabilities.preview_only(:requires_confirmation, obligations: [:audit_action])

      assert %Decision{
               status: :not_applicable,
               visibility: :hidden,
               reason_code: :not_in_surface
             } = Capabilities.not_applicable(:not_in_surface)
    end

    test "validates decision status and visibility" do
      assert_raise ArgumentError, ~r/unknown capability decision status :maybe/, fn ->
        Decision.new(status: :maybe)
      end

      assert_raise ArgumentError, ~r/unknown capability decision visibility :ghosted/, fn ->
        Decision.new(status: :deny, visibility: :ghosted)
      end
    end
  end

  describe "resolver helpers" do
    test "decide/3 supports function and module resolvers" do
      request = Capabilities.request(capability: "order.view", operation: :select)

      assert %Decision{status: :deny, reason_code: :blocked} =
               Capabilities.decide(
                 fn ^request ->
                   Capabilities.deny(:blocked)
                 end,
                 request
               )

      assert %Decision{
               status: :allow,
               reason_code: :module_allowed,
               metadata: %{capability: "order.view", context: %{surface: :test}}
             } =
               Capabilities.decide(ModuleResolver, request, resolver_context: %{surface: :test})
    end

    test "decide_many/3 uses module batch callbacks and preserves order" do
      requests = [
        Capabilities.request(capability: "order.view", operation: :select),
        Capabilities.request(capability: "order.export", operation: :export)
      ]

      assert [
               %Decision{
                 status: :allow,
                 reason_code: :batch_allowed,
                 metadata: %{capability: "order.view", context: %{surface: :test}}
               },
               %Decision{
                 status: :allow,
                 reason_code: :batch_allowed,
                 metadata: %{capability: "order.export", context: %{surface: :test}}
               }
             ] =
               Capabilities.decide_many(BatchResolver, requests,
                 resolver_context: %{surface: :test}
               )
    end

    test "decide_many/3 falls back to single decisions for function resolvers" do
      requests = [
        Capabilities.request(capability: "order.view", operation: :select),
        Capabilities.request(capability: "order.export", operation: :export)
      ]

      assert [
               %Decision{reason_code: "order.view"},
               %Decision{reason_code: "order.export"}
             ] =
               Capabilities.decide_many(
                 fn request ->
                   Capabilities.allow(request.capability)
                 end,
                 requests
               )
    end

    test "normalizes map and error resolver results" do
      request = Capabilities.request(capability: "order.view", operation: :select)

      assert %Decision{
               status: :deny,
               visibility: :disabled,
               reason_code: "manager_required",
               user_message: "Managers only."
             } =
               Capabilities.decide(
                 fn _request ->
                   %{
                     "status" => "disabled",
                     "code" => "manager_required",
                     "reason" => "Managers only."
                   }
                 end,
                 request
               )

      assert %Decision{
               status: :deny,
               visibility: :disabled,
               reason_code: :resolver_error,
               user_message: ":offline"
             } = Capabilities.decide(fn _request -> {:error, :offline} end, request)
    end
  end
end
