#!/usr/bin/env elixir

# Selecto Performance Test Runner
#
# This script provides a convenient way to run all performance tests and benchmarks.
#
# Usage:
#   mix run run_performance_tests.exs [options]
#
# Options:
#   --unit-tests    Run ExUnit performance tests
#   --benchmarks    Run Benchee benchmarks  
#   --memory        Run memory profiling
#   --all           Run all tests (default)
#   --help          Show this help

defmodule PerformanceTestRunner do
  def run(args \\ []) do
    options = parse_args(args)
    
    if options[:help] do
      print_help()
    else
      run_tests(options)
    end
  end

  defp parse_args(args) do
    default_options = %{
      unit_tests: false,
      benchmarks: false,
      memory: false,
      all: true,
      help: false
    }
    
    parsed = Enum.reduce(args, default_options, fn
      "--unit-tests", acc -> %{acc | unit_tests: true, all: false}
      "--benchmarks", acc -> %{acc | benchmarks: true, all: false}
      "--memory", acc -> %{acc | memory: true, all: false}
      "--all", acc -> %{acc | all: true}
      "--help", acc -> %{acc | help: true}
      "-h", acc -> %{acc | help: true}
      unknown, acc -> 
        IO.puts("⚠️  Unknown option: #{unknown}")
        acc
    end)
    
    # If all is true and no specific tests selected, enable all
    if parsed.all and not (parsed.unit_tests or parsed.benchmarks or parsed.memory) do
      %{parsed | unit_tests: true, benchmarks: true, memory: true}
    else
      parsed
    end
  end

  defp print_help do
    IO.puts("""
    🚀 Selecto Performance Test Runner
    
    This script runs comprehensive performance tests for Selecto's advanced join patterns.
    
    Usage:
      mix run run_performance_tests.exs [options]
    
    Options:
      --unit-tests    Run ExUnit-based performance tests (test/performance_test.exs)
      --benchmarks    Run Benchee benchmarks (benchmarks/join_patterns_benchmark.exs)
      --memory        Run memory profiling (benchmarks/memory_profiler.exs)
      --all           Run all tests (default if no specific options given)
      --help, -h      Show this help message
    
    Examples:
      mix run run_performance_tests.exs --benchmarks
      mix run run_performance_tests.exs --unit-tests --memory
      mix run run_performance_tests.exs --all
    
    Prerequisites:
      - For benchmarks: mix deps.get (to install optional benchee dependency)
      - For full test suite: mix test --include performance:true
    """)
  end

  defp run_tests(options) do
    IO.puts("🎯 Selecto Performance Test Suite")
    IO.puts("=" |> String.duplicate(50))
    
    results = []

    results = if options.unit_tests do
      IO.puts("\n📋 Running ExUnit Performance Tests...")
      result = run_unit_tests()
      [{"ExUnit Performance Tests", result} | results]
    else
      results
    end

    results = if options.benchmarks do
      IO.puts("\n⚡ Running Benchee Benchmarks...")
      result = run_benchmarks()
      [{"Benchee Benchmarks", result} | results]
    else
      results
    end

    results = if options.memory do
      IO.puts("\n🧠 Running Memory Profiling...")
      result = run_memory_profiling()
      [{"Memory Profiling", result} | results]
    else
      results
    end

    print_summary(results)
  end

  defp run_unit_tests do
    try do
      # Run the performance tests specifically
      {output, exit_code} = System.cmd("mix", ["test", "test/performance_test.exs", "--include", "performance:true"], 
        stderr_to_stdout: true)
      
      if exit_code == 0 do
        IO.puts("✅ ExUnit performance tests completed successfully")
        {:ok, output}
      else
        IO.puts("❌ ExUnit performance tests failed")
        IO.puts(output)
        {:error, output}
      end
    rescue
      error ->
        IO.puts("❌ Error running ExUnit tests: #{inspect(error)}")
        {:error, error}
    end
  end

  defp run_benchmarks do
    try do
      # Load and run the benchmark script
      Code.require_file("benchmarks/join_patterns_benchmark.exs")
      IO.puts("✅ Benchee benchmarks completed successfully")
      {:ok, "Benchmarks completed"}
    rescue
      error ->
        IO.puts("❌ Error running benchmarks: #{inspect(error)}")
        IO.puts("💡 Make sure benchee is installed: mix deps.get")
        {:error, error}
    end
  end

  defp run_memory_profiling do
    try do
      # Load and run the memory profiler
      Code.require_file("benchmarks/memory_profiler.exs")
      IO.puts("✅ Memory profiling completed successfully")
      {:ok, "Memory profiling completed"}
    rescue
      error ->
        IO.puts("❌ Error running memory profiling: #{inspect(error)}")
        {:error, error}
    end
  end

  defp print_summary(results) do
    IO.puts("\n" <> "=" |> String.duplicate(50))
    IO.puts("📊 Performance Test Summary")
    IO.puts("=" |> String.duplicate(50))
    
    if results == [] do
      IO.puts("No tests were run. Use --help for usage information.")
      return
    end

    Enum.each(results, fn {test_name, result} ->
      status = case result do
        {:ok, _} -> "✅ PASSED"
        {:error, _} -> "❌ FAILED"
      end
      IO.puts("#{test_name}: #{status}")
    end)
    
    passed_count = Enum.count(results, fn {_, result} -> match?({:ok, _}, result) end)
    total_count = length(results)
    
    IO.puts("\nResults: #{passed_count}/#{total_count} test suites passed")
    
    if passed_count == total_count do
      IO.puts("\n🎉 All performance tests completed successfully!")
      IO.puts("💡 Check generated reports in benchmarks/ directory for detailed results")
    else
      IO.puts("\n⚠️  Some performance tests failed. Check output above for details.")
      System.halt(1)
    end
  end
end

# Run if called directly
case System.argv() do
  args -> PerformanceTestRunner.run(args)
end