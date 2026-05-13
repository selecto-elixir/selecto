defmodule Selecto.CapabilitiesTest do
  use ExUnit.Case, async: true

  alias Selecto.Capabilities
  alias Selecto.Capabilities.{Decision, Request}

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
end
