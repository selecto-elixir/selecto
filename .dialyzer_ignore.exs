[
  # Execution API patterns that work correctly at runtime but appear problematic to Dialyzer
  # in test environment where database connections always fail
  ~r/Function execute!.*has no local return/,
  ~r/Function execute_one!.*has no local return/,
  ~r/invalid_contract.*execute!/,
  ~r/The pattern can never match.*execute/,
  ~r/pattern_match_cov.*execute/
]