defmodule Selecto.Domain.Examples do
  @moduledoc """
  Canonical domain examples used to keep ecosystem contract projections aligned.

  These examples are intentionally synthetic and database-free. They exercise the
  same contract vocabulary across core Selecto, SelectoComponents, and
  SelectoUpdato: query fields, filters, capabilities, choice sources, published
  views, writes, and domain actions.
  """

  @doc """
  A task/workflow domain with query, export, published-view, choice-source, and
  write/action surfaces.
  """
  @spec work_items() :: map()
  def work_items do
    %{
      name: "Work Items",
      source: %{
        source_table: "work_items",
        primary_key: :id,
        fields: [
          :id,
          :title,
          :state,
          :priority,
          :owner_id,
          :private_metric,
          :archived_at,
          :tenant_id
        ],
        columns: %{
          id: %{type: :integer, name: "ID"},
          title: %{type: :string, name: "Title"},
          state: %{
            type: :string,
            name: "State",
            choice_source: :state_choices,
            reference: %{
              choice_source: :state_choices,
              value_source: :state,
              caption_source: :state
            }
          },
          priority: %{type: :integer, name: "Priority"},
          owner_id: %{
            type: :integer,
            name: "Owner",
            choice_source: :owner_choices,
            reference: %{
              choice_source: :owner_choices,
              value_source: "owners.id",
              caption_source: "owners.name"
            }
          },
          private_metric: %{
            type: :integer,
            name: "Private Metric",
            capability: "work_items.private_metric"
          },
          archived_at: %{type: :utc_datetime, name: "Archived At"},
          tenant_id: %{type: :integer, name: "Tenant"}
        },
        associations: %{
          owner: %{
            queryable: :owners,
            field: :owner,
            owner_key: :owner_id,
            related_key: :id
          }
        }
      },
      schemas: %{
        owners: %{
          source_table: "users",
          primary_key: :id,
          fields: [:id, :name, :active, :tenant_id],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            active: %{type: :boolean},
            tenant_id: %{type: :integer}
          },
          associations: %{}
        }
      },
      joins: %{owner: %{type: :left}},
      default_selected: [:id, :title, :state, :priority],
      required_order_by: [{:priority, :desc}],
      filters: %{
        state: %{field: :state, type: :string, capability: "work_items.read"},
        owner: %{field: :owner_id, type: :integer, capability: "work_items.assign"},
        private_metric: %{
          field: :private_metric,
          type: :integer,
          capability: "work_items.private_metric"
        }
      },
      capabilities: %{
        "work_items.read" => %{operations: [:select, :filter]},
        "work_items.assign" => %{operations: [:choice_source, :update]},
        "work_items.private_metric" => %{operations: [:select, :filter, :export]},
        "work_items.archive" => %{operations: [:action, :update], action: :archive},
        "work_items.published_views" => %{operations: [:published_view]},
        "selecto.exports.download" => %{operations: [:export]},
        "selecto.exported_views.manage" => %{operations: [:create]},
        "selecto.scheduled_exports.manage" => %{operations: [:create]}
      },
      source_relationships: %{
        owner: %{
          target_domain: :users,
          source_field: :owner_id,
          target_field: :id,
          source_path: "owners",
          filters: [{:eq, "owners.active", true}]
        }
      },
      choice_sources: %{
        state_choices: %{
          domain: :work_item_states,
          value_field: :state,
          label_field: :state,
          presentation: %{control: :select, mode: :static, cardinality: :one}
        },
        owner_choices: %{
          domain: :users,
          source_relationship: :owner,
          value_field: :id,
          label_field: :name,
          source_path: "owners",
          value_source: "owners.id",
          caption_source: "owners.name",
          filters: [{:eq, "owners.tenant_id", {:context, :tenant_id}}],
          order_by: ["owners.name"],
          presentation: %{control: :autocomplete, mode: :searchable, cardinality: :one},
          constraint_policy: %{domain_of_interest: :fail_closed},
          capability: "work_items.assign"
        }
      },
      published_views: %{
        manager_rollup: %{
          database_name: "reporting.work_item_manager_rollup",
          kind: :materialized_view,
          query: fn selecto -> selecto end,
          columns: %{
            state: %{type: :string},
            total: %{type: :integer}
          },
          refresh: %{concurrently: true},
          capability: "work_items.published_views"
        }
      },
      writes: %{
        operations: %{
          update: %{enabled: true, require_filter: true},
          soft_delete: %{enabled: true, require_filter: true}
        },
        fields: %{
          state: %{updatable: true},
          priority: %{updatable: true},
          owner_id: %{updatable: true},
          archived_at: %{updatable: true}
        },
        scope: %{
          tenant: %{required: true, field: :tenant_id, satisfied_by: [:trusted_context]}
        },
        transitions: %{
          state: %{
            "open" => ["in_progress", "archived"],
            "in_progress" => ["done", "archived"],
            "done" => ["archived"],
            "archived" => []
          }
        }
      },
      actions: %{
        archive: %{
          label: "Archive work item",
          type: :transition,
          scope: :row,
          capability: "work_items.archive",
          inputs: %{
            reason: %{type: :string, required: false}
          },
          transition: %{field: :state, from: "done", to: "archived"},
          execution: %{
            kind: :updato,
            operation: :update,
            set: %{state: "archived"}
          }
        }
      }
    }
  end

  @doc """
  A camp registration domain proving the same contract surfaces outside the
  Work Items vocabulary.
  """
  @spec camp_registrations() :: map()
  def camp_registrations do
    %{
      name: "Camp Registrations",
      source: %{
        source_table: "camp_registrations",
        primary_key: :id,
        fields: [
          :id,
          :camper_first_name,
          :status,
          :medical_form_received,
          :balance_cents,
          :documents_complete,
          :checked_in_at,
          :cabin_id,
          :tenant_id
        ],
        columns: %{
          id: %{type: :integer, name: "ID"},
          camper_first_name: %{type: :string, name: "Camper"},
          status: %{type: :string, name: "Status"},
          medical_form_received: %{type: :boolean, name: "Medical Form"},
          balance_cents: %{type: :integer, name: "Balance"},
          documents_complete: %{type: :boolean, name: "Documents Complete"},
          checked_in_at: %{type: :utc_datetime, name: "Checked In At"},
          cabin_id: %{
            type: :integer,
            name: "Cabin",
            choice_source: :cabin_choices,
            reference: %{
              choice_source: :cabin_choices,
              value_source: "cabins.id",
              caption_source: "cabins.name"
            }
          },
          tenant_id: %{type: :integer, name: "Tenant"}
        },
        associations: %{
          cabin: %{
            queryable: :cabins,
            field: :cabin,
            owner_key: :cabin_id,
            related_key: :id
          }
        }
      },
      schemas: %{
        cabins: %{
          source_table: "camp_cabins",
          primary_key: :id,
          fields: [:id, :name, :session_id, :open],
          columns: %{
            id: %{type: :integer},
            name: %{type: :string},
            session_id: %{type: :integer},
            open: %{type: :boolean}
          },
          associations: %{}
        }
      },
      joins: %{cabin: %{type: :left}},
      default_selected: [
        :id,
        :camper_first_name,
        :status,
        :medical_form_received,
        :balance_cents
      ],
      filters: %{
        status: %{field: :status, type: :string, capability: "camp.registrations.read"},
        cabin: %{field: :cabin_id, type: :integer, capability: "camp.cabins.assign"}
      },
      capabilities: %{
        "camp.registrations.read" => %{operations: [:select, :filter]},
        "camp.cabins.assign" => %{operations: [:choice_source, :update]},
        "camp.check_in" => %{operations: [:action, :update], action: :check_in_camper},
        "camp.published_views" => %{operations: [:published_view]},
        "selecto.exports.download" => %{operations: [:export]},
        "selecto.exported_views.manage" => %{operations: [:create]},
        "selecto.scheduled_exports.manage" => %{operations: [:create]}
      },
      source_relationships: %{
        cabin: %{
          target_domain: :camp_cabins,
          source_field: :cabin_id,
          target_field: :id,
          source_path: "cabins",
          filters: [{:eq, "cabins.open", true}]
        }
      },
      choice_sources: %{
        cabin_choices: %{
          domain: :camp_cabins,
          source_relationship: :cabin,
          value_field: :id,
          label_field: :name,
          source_path: "cabins",
          value_source: "cabins.id",
          caption_source: "cabins.name",
          filters: [{:eq, "cabins.session_id", {:context, :session_id}}],
          order_by: ["cabins.name"],
          presentation: %{control: :table_picker, mode: :async, cardinality: :one},
          constraint_policy: %{domain_of_interest: :fail_closed},
          capability: "camp.cabins.assign"
        }
      },
      published_views: %{
        check_in_roster: %{
          database_name: "reporting.camp_check_in_roster",
          kind: :view,
          query: fn selecto -> selecto end,
          columns: %{
            cabin_id: %{type: :integer},
            registered_count: %{type: :integer}
          },
          capability: "camp.published_views"
        }
      },
      writes: %{
        operations: %{
          update: %{enabled: true, require_filter: true}
        },
        fields: %{
          status: %{updatable: true},
          documents_complete: %{updatable: true},
          checked_in_at: %{updatable: true},
          cabin_id: %{updatable: true}
        },
        scope: %{
          tenant: %{required: true, field: :tenant_id, satisfied_by: [:trusted_context]}
        },
        transitions: %{
          status: %{
            "registered" => ["checked_in", "cancelled"],
            "checked_in" => [],
            "cancelled" => []
          }
        }
      },
      actions: %{
        check_in_camper: %{
          label: "Check in camper",
          type: :transition,
          scope: :row,
          capability: "camp.check_in",
          inputs: %{
            documents_complete: %{type: :boolean, required: true, default: true},
            checked_in_at: %{type: :utc_datetime, required: true}
          },
          transition: %{field: :status, from: "registered", to: "checked_in"},
          execution: %{
            kind: :updato,
            operation: :update,
            set: %{status: "checked_in"}
          }
        }
      }
    }
  end
end
