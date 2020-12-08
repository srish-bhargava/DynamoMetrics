defmodule CrashFuzzer do
  use GenServer
  require Logger

  # client
  def start() do
    GenServer.start_link(__MODULE__, nil)
  end

  def enable(pid, mtt_fail, mtt_recover, procs) do
    GenServer.cast(pid, {:enable, mtt_fail, mtt_recover, procs})
  end

  # server

  @impl true
  def init(_) do
    {:ok, %{mtt_fail: nil, mtt_recover: nil, procs: nil}}
  end

  defp random_from_exp_distribution(mean) do
    if mean > 0 do
      Statistics.Distributions.Exponential.rand(1.0 / mean)
      |> Float.round()
      |> trunc
    else
      0
    end
  end

  def random_tt_fail(state) do
    random_from_exp_distribution(state.mtt_fail)
  end

  def random_tt_recover(state) do
    random_from_exp_distribution(state.mtt_recover)
  end

  @impl true
  def handle_cast({:enable, mtt_fail, mtt_recover, procs}, state) do
    if state.mtt_fail != nil or state.mtt_recover != nil do
      raise "Enabled in-progress CrashFuzzer"
    end

    state = %{
      state
      | mtt_fail: mtt_fail,
        mtt_recover: mtt_recover,
        procs: procs
    }

    # nothing has failed yet
    # start a fail timer for every fuzzable node
    for proc <- procs do
      tt_fail = random_tt_fail(state)
      Process.send_after(self(), {:fail, proc}, tt_fail)
    end

    {:noreply, state, :hibernate}
  end

  @impl true
  def handle_info({:fail, proc}, state) do
    # fail proc
    send(proc, {:from_crash_fuzzer, :crash})

    # start timer for recovery
    tt_recover = random_tt_recover(state)
    Process.send_after(self(), {:recover, proc}, tt_recover)

    {:noreply, state, :hibernate}
  end

  @impl true
  def handle_info({:recover, proc}, state) do
    # recover proc
    send(proc, {:from_crash_fuzzer, :recover})

    # start timer for next crash
    tt_fail = random_tt_fail(state)
    Process.send_after(self(), {:fail, proc}, tt_fail)

    {:noreply, state, :hibernate}
  end
end
