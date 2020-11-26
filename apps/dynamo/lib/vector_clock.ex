defmodule VectorClock do
  @moduledoc """
  Logical vector clocks for managing versions.
  """

  @type t() :: %{required(any()) => non_neg_integer()}

  @doc """
  Return a new, empty, vector clock.

      iex> VectorClock.new()
      %{}
  """
  @spec new() :: t()
  def new() do
    %{}
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
  @spec combine(t(), t()) :: t()
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
  @spec tick(t(), any()) :: t()
  def tick(clock, proc) do
    Map.update(clock, proc, 1, &(&1 + 1))
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

      iex> VectorClock.compare(%{a: 4, b: 7}, %{a: 5, b: 7})
      :before
  """
  @spec compare(t(), t()) :: :before | :after | :concurrent
  def compare(clock_1, clock_2) do
    keys =
      MapSet.union(MapSet.new(Map.keys(clock_1)), MapSet.new(Map.keys(clock_2)))

    comparisons =
      for key <- keys, into: MapSet.new() do
        val_1 = Map.get(clock_1, key, 0)
        val_2 = Map.get(clock_2, key, 0)

        cond do
          val_1 < val_2 -> :before
          val_1 > val_2 -> :after
          val_1 == val_2 -> :equal
        end
      end

    cond do
      # before at some points and after at others => no order
      :before in comparisons and :after in comparisons -> :concurrent
      # completely equal
      :equal in comparisons and MapSet.size(comparisons) == 1 -> :concurrent
      # before at some points and equal at others
      :before in comparisons -> :before
      # after at some points and equal at others
      true -> :after
    end
  end

  @doc """
  Check if clock_1 is before clock_2.

      iex> VectorClock.before?(%{a: 4, b: 2}, %{a: 5, b: 7})
      true

      iex> VectorClock.before?(%{a: 4, b: 7}, %{a: 5, b: 7})
      true

      iex> VectorClock.before?(%{a: 4, b: 8}, %{a: 5, b: 7})
      false

      iex> VectorClock.before?(%{a: 4, b: 2, c: 2}, %{a: 5, b: 7})
      false
  """
  @spec before?(t(), t()) :: boolean()
  def before?(clock_1, clock_2) do
    compare(clock_1, clock_2) == :before
  end

  @doc """
  Check if clock_1 is after clock_2.

      iex> VectorClock.after?(%{a: 5, b: 7}, %{a: 4, b: 2})
      true

      iex> VectorClock.after?(%{a: 5, b: 7}, %{a: 4, b: 7})
      true

      iex> VectorClock.after?(%{a: 5, b: 7}, %{a: 4, b: 8})
      false

      iex> VectorClock.after?(%{a: 5, b: 7}, %{a: 4, b: 2, c: 2})
      false
  """
  @spec after?(t(), t()) :: boolean()
  def after?(clock_1, clock_2) do
    compare(clock_1, clock_2) == :after
  end

  @doc """
  Check if clock_1 is concurrent with clock_2.

      iex> VectorClock.concurrent?(%{a: 5, b: 7}, %{a: 4, b: 2})
      false

      iex> VectorClock.concurrent?(%{a: 5, b: 7}, %{a: 4, b: 8})
      true

      iex> VectorClock.concurrent?(%{a: 5, b: 7}, %{a: 4, b: 2, c: 2})
      true
  """
  @spec concurrent?(t(), t()) :: boolean()
  def concurrent?(clock_1, clock_2) do
    compare(clock_1, clock_2) == :concurrent
  end
end
