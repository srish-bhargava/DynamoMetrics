defmodule DynamoNode do
  @moduledoc """
  A replica node in a dynamo cluster.
  """

  alias ExHashRing.HashRing

  # override Kernel's functions with Emulation's
  import Emulation, only: [spawn: 2, send: 2, timer: 1, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  # The state of each node
  defstruct(
    # node id
    id: nil,
    # local storage of key-(value, clock) pairs
    store: nil,
    # all nodes in the cluster
    nodes: nil,
    # hash ring
    ring: nil,
    # parameters from dynamo
    # for minimum participants in read/write
    n: nil,
    r: nil,
    w: nil,
    # logical clock for versioning
    vector_clock: nil
  )

  @doc """
  Set up node and start serving requests.
  """
  def start(id, store, nodes, n, r, w) do
    Logger.info("Starting node #{inspect(id)}")

    state = %DynamoNode{
      id: id,
      store: store,
      nodes: nodes,
      ring: HashRing.new(nodes, 1),
      n: n,
      r: r,
      w: w,
      vector_clock: VectorClock.new(nodes)
    }

    # TODO start anti-entropy in a background process

    listener(state)
  end

  @doc """
  Listen and serve requests, forever.
  """
  def listener(state) do
    receive do
      # client requests
      {client, {:get, key}} ->
        Logger.info(
          "Received a :get request for key=#{inspect(key)} " <>
            "from client=#{inspect(client)}"
        )

        raise "TODO"

      {client, {:put, key, value}} ->
        Logger.info(
          "Received a :put request for key=#{inspect(key)}, " <>
            "value=#{inspect(value)} from client=#{inspect(client)}"
        )

        raise "TODO"

      # internal requests
      {coordinator, {:get_internal, key}} ->
        Logger.info(
          "Received a :get_internal request for key=#{inspect(key)} " <>
            "from coordinator=#{inspect(coordinator)}"
        )

        raise "TODO"

      {coordinator, {:put_internal, key, value, clock}} ->
        Logger.info(
          "Received a :put_internal request for key=#{inspect(key)}, " <>
            "value=#{inspect(value)}, clock=#{inspect(clock)} from " <>
            "coordinator=#{inspect(coordinator)}"
        )

        raise "TODO"
    end

    listener(state)
  end

  @doc """
  Respond to a :get request for `key`,
  ASSUMING we are the co-ordinator for this key.

  Steps:
  1. Request all versions of data from the top `n` nodes in
       the preference list for key (regardless of whether
       we believe them to be healthy or not).
  2. Wait for r responses.
  3. Return all latest concurrent versions of the key's values
       received.
  """
  def get(state, key) do
    raise "TODO get"
  end

  @doc """
  Respond to a :put request for `key`,
  ASSUMING we are the co-ordinator for this key.

  Steps:
  1. Increment vector_clock
  2. Write to own store
  3. Send {key,value,vector_clock} to top `n` nodes in
       the preference list for key
  4. Wait for responses.
  5. If (w - 1) responses received, return success,
       otherwise failure.
  """
  def put(state, key, value) do
    raise "TODO put"
  end
end
