run_db_tests? =
  System.get_env("SELECTO_RUN_DB_TESTS", "0")
  |> String.downcase()
  |> then(&(&1 in ["1", "true", "yes"]))

exclude_tags =
  if run_db_tests? do
    []
  else
    [requires_db: true]
  end

ExUnit.start(exclude: exclude_tags)
Mneme.start()
