defmodule VectorClock do
  @moduledoc """
  Logical vector clocks for managing versions.
  """

  @doc """
  Return a new vector clock with time at all nodes initialized to 0.

      iex> VectorClock.new([:a, :b, :c])
      %{a: 0, b: 0, c: 0}
  """
  def new(nodes) do
    for node <- nodes, into: %{}, do: {node, 0}
  end

  @doc """
  Combine vector clocks: this is called whenever a
  message is received, and should return the clock
  from combining the two.

      iex> VectorClock.combine(%{a: 1, b: 0, c: 3}, %{a: 5, b: 2, c: 1})
      %{a: 5, b: 2, c: 3}

      iex> VectorClock.combine(%{a: 1, b: 0, c: 3}, %{a: 5, b: 2, c: 1, d: 9})
      %{a: 5, b: 2, c: 3, d: 9}
  """
  def combine(current, received) do
    Map.merge(current, received, fn _k, c, r -> max(c, r) end)
  end

  @doc """
  Increment the clock by one tick for a given process.

      iex> VectorClock.tick(%{a: 1, b: 0, c: 3}, :a)
      %{a: 2, b: 0, c: 3}

      iex> VectorClock.tick(%{a: 1, b: 0, c: 3}, :b)
      %{a: 1, b: 1, c: 3}
  """
  def tick(clock, proc) do
    Map.update!(clock, proc, &(&1 + 1))
  end

  @doc """
  Compare two vector clocks,
  returning one of {:before, :after, :concurrent}

      iex> VectorClock.compare(%{a: 1, b: 0, c: 3}, %{a: 5, b: 2, c: 1})
      :concurrent

      iex> VectorClock.compare(%{a: 1, b: 0, c: 3}, %{a: 5, b: 2, c: 6})
      :before

      iex> VectorClock.compare(%{a: 5, b: 2, c: 6}, %{a: 1, b: 0, c: 3})
      :after

      iex> VectorClock.compare(%{a: 5, b: 2, c: 6}, %{a: 1, b: 0, c: 3, d: 9})
      :concurrent

      iex> VectorClock.compare(%{a: 5, b: 2, c: 6, d: 9}, %{a: 1, b: 0, c: 3})
      :after
  """
  def compare(clock_1, clock_2) do
    keys =
      MapSet.union(MapSet.new(Map.keys(clock_1)), MapSet.new(Map.keys(clock_2)))

    pointwise_comparisons =
      for key <- keys do
        val_1 = Map.get(clock_1, key, 0)
        val_2 = Map.get(clock_2, key, 0)

        cond do
          val_1 < val_2 -> :before
          val_1 > val_2 -> :after
          val_1 == val_2 -> :concurrent
        end
      end

    cond do
      Enum.all?(pointwise_comparisons, fn x -> x == :before end) -> :before
      Enum.all?(pointwise_comparisons, fn x -> x == :after end) -> :after
      true -> :concurrent
    end
  end
end
