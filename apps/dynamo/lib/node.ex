defmodule DynamoNode do
  @moduledoc """
  A replica node in a DynamoDB cluster.
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
    Logger.metadata(id: id)

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
  Get the preference list for a particular key
  (i.e. the top `n` nodes in the ring for this key).
  """
  def get_preference_list(state, key) do
    HashRing.find_nodes(state.ring, key, state.n)
  end

  @doc """
  Get the coordinator for a particular key
  (i.e. the top node in the ring for this key).
  """
  def get_coordinator(state, key) do
    HashRing.find_node(state.ring, key)
  end

  @doc """
  Listen and serve requests, forever.
  """
  def listener(state) do
    # TODO figure out when we should update vector_clock
    receive do
      # client requests
      {client, {:get, key}} ->
        Logger.info(
          "Received a :get request for key=#{inspect(key)} " <>
            "from client=#{inspect(client)}"
        )

        coordinator = get_coordinator(state, key)

        if coordinator != state.id do
          # we are not the coordinator, so redirect to them
          send(coordinator, {:redirect, {client, {:get, key}}})
        else
          # we are the coordinator, so process the request
          get_as_coordinator(state, key)
        end

      {client, {:put, key, value}} ->
        Logger.info(
          "Received a :put request for key=#{inspect(key)}, " <>
            "value=#{inspect(value)} from client=#{inspect(client)}"
        )

        coordinator = get_coordinator(state, key)

        if coordinator != state.id do
          # we are not the coordinator, so redirect to them
          send(coordinator, {:redirect, {client, {:put, key, value}}})
        else
          # we are the coordinator, so process the request
          put_as_coordinator(state, key, value)
        end

      # redirects from other nodes
      {node, {:redirect, {client, {:get, key}}}} ->
        Logger.info(
          "Received a redirect from #{inspect(node)} for a :get request " <>
            "for key=#{inspect(key)} from client=#{inspect(client)}"
        )

        # we must be the coordinator for this key
        get_as_coordinator(state, key)

      {node, {:redirect, {client, {:put, key, value}}}} ->
        Logger.info(
          "Received a redirect from #{inspect(node)} for a :get request " <>
            "for key=#{inspect(key)}, value=#{inspect(value)} " <>
            "from client=#{inspect(client)}"
        )

        # we must be the coordinator for this key
        put_as_coordinator(state, key, value)

      # coordinator requests
      {coordinator, {:get_internal_request, key}} ->
        Logger.info(
          "Received a :get_internal request for key=#{inspect(key)} " <>
            "from coordinator=#{inspect(coordinator)}"
        )

        raise "TODO"

      {coordinator, {:put_internal_request, key, value, clock}} ->
        Logger.info(
          "Received a :put_internal request for key=#{inspect(key)}, " <>
            "value=#{inspect(value)}, clock=#{inspect(clock)} from " <>
            "coordinator=#{inspect(coordinator)}"
        )

        raise "TODO"

      # node responses to coordinator requests
      {node, {:get_internal_response, key, value, clock}} ->
        Logger.info(
          "Received a :get_internal response for key=#{inspect(key)}, " <>
            "value=#{inspect(value)}, clock=#{inspect(clock)} from " <>
            "node=#{inspect(node)}"
        )

        raise "TODO"

      {node, {:put_internal_response, key}} ->
        Logger.info(
          "Received a :put_internal response for key=#{inspect(key)} from " <>
            "node=#{inspect(node)}"
        )

        raise "TODO"
    end

    listener(state)
  end

  @doc """
  Respond to a :get request for `key`,
  assuming we are the co-ordinator for this key.

  Steps:
  1. Request all versions of data from the top `n` nodes in
       the preference list for key (regardless of whether
       we believe them to be healthy or not).
  2. Wait for r responses.
  3. Return all latest concurrent versions of the key's values
       received.
  """
  def get_as_coordinator(state, key) do
    # This function should return immediately, and not keep the
    # receive loop waiting while we get all `r` responses.
    # Thus we probably need to spawn another process.
    # Such a process would not need to share any state with this one,
    # aside from the ring.
    raise "TODO get"
  end

  @doc """
  Respond to a :put request for `key`,
  assuming we are the co-ordinator for this key.

  Steps:
  1. Increment vector_clock
  2. Write to own store
  3. Send {key,value,vector_clock} to top `n` nodes in
       the preference list for key
  4. Wait for responses.
  5. If (w - 1) responses received, return success,
       otherwise failure.
  """
  def put_as_coordinator(state, key, value) do
    raise "TODO put"
  end
end
