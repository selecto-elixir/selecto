# Performance Testing Guide

This guide explains how to run and interpret Selecto's performance tests for advanced join patterns.

## Overview

Selecto includes comprehensive performance testing to ensure that complex join patterns remain efficient as the library scales. The performance test suite covers:

- **Star Schema Dimensions** - OLAP-style fact-to-dimension joins
- **Snowflake Schema Patterns** - Normalized dimension chains
- **Hierarchical Joins** - Adjacency list and materialized path patterns
- **Many-to-Many Tagging** - Faceted filtering and tag aggregation
- **Mixed Join Patterns** - Complex combinations of all join types
- **Memory Usage Analysis** - Memory efficiency and leak detection

## Quick Start

### Running All Performance Tests

```bash
# Run everything (requires benchee dependency)
mix deps.get
mix run run_performance_tests.exs --all

# Or run specific test types
mix run run_performance_tests.exs --unit-tests
mix run run_performance_tests.exs --benchmarks  
mix run run_performance_tests.exs --memory
```

### Running Individual Test Suites

```bash
# ExUnit performance tests (no extra dependencies)
mix test test/performance_test.exs --include performance:true

# Benchee benchmarks (requires benchee)
mix run benchmarks/join_patterns_benchmark.exs

# Memory profiler
mix run benchmarks/memory_profiler.exs
```

## Test Types

### 1. ExUnit Performance Tests

Located in `test/performance_test.exs`, these tests validate that query generation stays within acceptable performance bounds.

**Key Metrics:**
- Query generation time (should be < 50ms for most patterns)
- Memory usage during test runs
- SQL generation correctness under load

**Example Output:**
```
📊 Star Schema Performance:
  Average query generation time: 12.3ms
  Total dimensions: 50
  Select fields: 25, Filters: 20, Group by: 15

🌳 Adjacency List Hierarchy Performance:
  Average query generation time: 45.2ms
  Hierarchy depth: 10 levels
```

**Interpreting Results:**
- ✅ **Green zone**: < 50ms average for star schema, < 100ms for hierarchical
- ⚠️ **Yellow zone**: 50-100ms star schema, 100-150ms hierarchical  
- ❌ **Red zone**: > 100ms star schema, > 150ms hierarchical

### 2. Benchee Benchmarks

Located in `benchmarks/join_patterns_benchmark.exs`, these provide detailed statistical analysis using the Benchee library.

**Key Metrics:**
- Iterations per second
- Average execution time
- Standard deviation
- Memory consumption per operation

**Example Output:**
```
Name                           ips        average  deviation         median         99th %
Star Schema Query           2.15 K      464.84 μs    ±12.45%      445.32 μs      723.45 μs
Snowflake Schema Query      1.89 K      528.91 μs    ±15.23%      501.67 μs      834.12 μs
Adjacency List Hierarchy    1.12 K      894.23 μs    ±18.76%      856.34 μs     1234.56 μs
Many-to-Many Tagging        1.67 K      598.45 μs    ±13.89%      567.89 μs      912.34 μs
Mixed Join Patterns         0.89 K     1123.45 μs    ±21.34%     1089.67 μs     1567.89 μs
```

**Interpreting Results:**
- **ips (iterations per second)**: Higher is better
- **average**: Lower is better for execution time
- **deviation**: Lower is better (more consistent performance)
- **median vs 99th %**: Large gaps indicate inconsistent performance

### 3. Memory Profiler

Located in `benchmarks/memory_profiler.exs`, this analyzes memory usage patterns and detects potential leaks.

**Key Metrics:**
- Memory increase during test execution
- Cleanup efficiency (garbage collection effectiveness)
- Net memory usage after cleanup

**Example Output:**
```
Testing: Simple Star Schema
  Memory increase: 2.3MB total
  Process memory: 1.8MB
  Binary memory: 0.5MB
  Cleanup recovered: 2.1MB

💡 Memory Optimization Recommendations:
• Deep Hierarchy (10 levels) uses the most memory (5.2MB)
• ✅ Good memory cleanup across all patterns
• For large domains, consider pagination or result limiting
```

**Interpreting Results:**
- **Memory increase**: Expected during test execution
- **Cleanup recovered**: Should be > 90% of memory increase
- **Net memory usage**: Should be minimal after cleanup

## Performance Baselines

### Acceptable Performance Ranges

| Pattern Type | Query Generation | Memory Usage | Notes |
|--------------|------------------|--------------|-------|
| Star Schema | < 50ms | < 5MB | Fastest pattern, optimized for OLAP |
| Snowflake Schema | < 75ms | < 10MB | More complex due to normalization chains |
| Adjacency Hierarchy | < 100ms | < 15MB | CTE generation overhead |
| Materialized Path | < 60ms | < 8MB | More efficient than adjacency |
| Many-to-Many Tagging | < 80ms | < 12MB | Aggregation complexity |
| Mixed Patterns | < 150ms | < 25MB | Most complex, combines all patterns |

### Performance Degradation Indicators

**Query Generation Time:**
- Sudden spikes in average time
- High standard deviation (inconsistent performance) 
- Linear growth with complexity (should be sub-linear)

**Memory Usage:**
- Memory leaks (poor cleanup efficiency)
- Excessive memory for simple operations
- Memory growth that doesn't stabilize

## Optimization Strategies

### For Slow Query Generation

1. **Reduce Join Complexity**
   ```elixir
   # Instead of deep nesting
   "customer[region][country][continent]"
   
   # Consider denormalization
   "customer[continent]"  # Pre-computed
   ```

2. **Limit Hierarchy Depth**
   ```elixir
   joins: %{
     category: %{
       type: :hierarchical,
       hierarchy_type: :adjacency_list,
       depth_limit: 5  # Prevent excessive recursion
     }
   }
   ```

3. **Use Selective Filters Early**
   ```elixir
   selecto
   |> Selecto.filter([
     {"date[year]", 2024},        # Most selective first
     {"active", true},            # Indexed fields
     {"category_level", {:lte, 3}} # Limit complexity
   ])
   ```

### For High Memory Usage

1. **Implement Result Pagination**
   ```elixir
   selecto
   |> Selecto.limit(1000)
   |> Selecto.offset(page * 1000)
   ```

2. **Reduce Select Field Count**
   ```elixir
   # Only select what you need
   |> Selecto.select(["essential_field", {:func, "count", ["*"]}])
   ```

3. **Use Materialized Path Instead of Adjacency**
   ```elixir
   # More memory efficient for deep hierarchies
   hierarchy_type: :materialized_path
   ```

## Continuous Integration

### Adding Performance Tests to CI

```yaml
# In your CI pipeline
- name: Run Performance Tests
  run: |
    mix deps.get
    mix test test/performance_test.exs --include performance:true
    mix run benchmarks/memory_profiler.exs
```

### Performance Regression Detection

```elixir
# Add assertions to catch regressions
test "performance regression check" do
  {time_us, _result} = :timer.tc(fn ->
    # Your query generation here
  end)
  
  time_ms = time_us / 1000
  
  # Fail if performance degrades significantly
  assert time_ms < 50.0, "Query generation too slow: #{time_ms}ms"
end
```

## Troubleshooting

### Common Issues

**"Benchee not found" Error:**
```bash
# Install optional benchee dependency
mix deps.get
```

**Performance Tests Timing Out:**
```elixir
# Increase test timeout in test/performance_test.exs
@tag timeout: 300_000  # 5 minutes
```

**Memory Profiler Crashes:**
```elixir
# Reduce domain size in memory tests
domain = build_large_domain(10)  # Instead of 100
```

### Performance Investigation

1. **Profile Specific Queries:**
   ```elixir
   :fprof.start()
   :fprof.trace(:start)
   
   # Run your query
   Selecto.to_sql(query)
   
   :fprof.trace(:stop)
   :fprof.profile()
   :fprof.analyse()
   ```

2. **Memory Analysis:**
   ```elixir
   # Before operation
   before = :erlang.memory()
   
   # Run operation
   result = expensive_operation()
   
   # After operation  
   after_mem = :erlang.memory()
   
   # Check difference
   IO.inspect(after_mem[:total] - before[:total])
   ```

3. **SQL Query Analysis:**
   ```elixir
   {sql, params} = Selecto.to_sql(query)
   IO.puts("Generated SQL:")
   IO.puts(sql)
   IO.puts("Parameters: #{inspect(params)}")
   ```

## Best Practices

1. **Run performance tests regularly** during development
2. **Set performance budgets** for acceptable response times
3. **Monitor memory usage** in long-running applications
4. **Profile before optimizing** - measure first, optimize second
5. **Test with realistic data sizes** that match production
6. **Document performance characteristics** for different join patterns

This performance testing framework ensures Selecto maintains excellent performance as it scales to handle complex analytical workloads.