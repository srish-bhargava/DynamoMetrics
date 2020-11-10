defmodule DynamoNode do
  @moduledoc """
  A replica node in a DynamoDB cluster.
  """

  alias ExHashRing.HashRing

  # override Kernel's functions with Emulation's
  import Emulation, only: [send: 2, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  # The state of each node
  defstruct(
    # node id
    id: nil,
    # local storage of key-(value, clock) pairs
    # only stores concurrent versions
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
    # pending client get requests being handled
    # by this node as coordinator.
    # `pending_gets` is of the form:
    # %{client_nonce => %{client: client, responses: %{node => values}}}
    # Importantly, if a request has been dispatched to other nodes
    # but no coordinator responses have been received yet, then the
    # appropriate client_nonce WILL BE PRESENT and will map to {client, %{}}.
    # This helps distinguish this case with the case when an enough
    # responses have already been received and client_nonce purged
    # from this map.
    pending_gets: nil,
    # pending client put requests being handled
    # by this node as coordinator.
    # `pending_puts` is of the form:
    # %{client_nonce => %{client: client, responses: [node]}}
    # Similar to `pending_gets`, a request that's pending but for which
    # no coordinator responses have been received yet WILL have its
    # client_nonce be present and map to %{client: client, responses: []}
    pending_puts: nil,
    # logical clock for versioning
    vector_clock: nil
  )

  @doc """
  Set up node and start serving requests.
  """
  def start(id, data, nodes, n, r, w) do
    Logger.info("Starting node #{inspect(id)}")
    Logger.metadata(id: id)

    vector_clock = VectorClock.new(nodes)

    # convert data from a key-value map to a versioned store
    store = Map.new(data, fn {k, v} -> {k, [{v, vector_clock}]} end)

    state = %DynamoNode{
      id: id,
      store: store,
      nodes: nodes,
      ring: HashRing.new(nodes, 1),
      n: n,
      r: r,
      w: w,
      pending_gets: %{},
      pending_puts: %{},
      vector_clock: vector_clock
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
      {client, %ClientRequest.Get{key: key} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        coordinator = get_coordinator(state, key)

        if coordinator != state.id do
          # we are not the coordinator, so redirect to them
          send(coordinator, %RedirectedClientRequest{
            client: client,
            request: msg
          })

          listener(state)
        else
          # we are the coordinator, so process the request
          state = get_as_coordinator(state, client, msg)
          listener(state)
        end

      {client, %ClientRequest.Put{key: key} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        coordinator = get_coordinator(state, key)

        if coordinator != state.id do
          # we are not the coordinator, so redirect to them
          send(coordinator, %RedirectedClientRequest{
            client: client,
            request: msg
          })

          listener(state)
        else
          # we are the coordinator, so process the request
          state = put_as_coordinator(state, client, msg)
          listener(state)
        end

      # redirects from other nodes
      {node,
       %RedirectedClientRequest{
         client: client,
         request: %ClientRequest.Get{} = orig_msg
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        # we must be the coordinator for this key
        state = get_as_coordinator(state, client, orig_msg)
        listener(state)

      {node,
       %RedirectedClientRequest{
         client: client,
         request: %ClientRequest.Put{} = orig_msg
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        # we must be the coordinator for this key
        state = put_as_coordinator(state, client, orig_msg)
        listener(state)

      # coordinator requests
      {coordinator, %CoordinatorRequest.Get{nonce: nonce, key: key} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(coordinator)}")

        values = Map.get(state.store, key)

        send(coordinator, %CoordinatorResponse.Get{nonce: nonce, values: values})

        listener(state)

      {coordinator,
       %CoordinatorRequest.Put{
         nonce: nonce,
         key: key,
         value: value,
         clock: clock
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(coordinator)}")

        state = put(state, key, value, clock)
        send(coordinator, %CoordinatorResponse.Put{nonce: nonce})
        listener(state)

      # node responses to coordinator requests
      {node, %CoordinatorResponse.Get{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = get_resp_as_coordinator(state, node, msg)
        listener(state)

      {node,
       %CoordinatorResponse.Put{
         nonce: nonce
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        raise "TODO"
    end
  end

  @doc """
  Respond to `client`'s `get` request, assuming we are
  the co-ordinator for the requested key.

  Steps:
  -- These steps happen synchronously
  1. Request all versions of data from the top `n` nodes in
       the preference list for key (regardless of whether
       we believe them to be healthy or not).

  -- These steps happen asynchronously in `get_resp_as_coordinator`
     whenever we receive a CoordinatorResponse.Get msg
  2. Wait for r responses.
  3. Return all latest concurrent versions of the key's values
       received.

  TODO Return failure to client on a timeout?
  """
  def get_as_coordinator(state, client, %ClientRequest.Get{
        nonce: nonce,
        key: key
      }) do
    Enum.each(get_preference_list(state, key), fn node ->
      # DO send get request to self
      send(node, %CoordinatorRequest.Get{nonce: nonce, key: key})
    end)

    %{
      state
      | pending_gets:
          Map.put(state.pending_gets, nonce, %{client: client, responses: %{}})
    }
  end

  @doc """
  Process a CoordinatorResponse.Get msg

  Add it to the list of responses in `pending_gets`.
  If we have `r` or more responses for the corresponding client request,
  remove this request from `pending_gets` and return all latest values to
  the client.
  """
  def get_resp_as_coordinator(state, node, %CoordinatorResponse.Get{
        nonce: nonce,
        values: values
      }) do
    if not Map.has_key?(state.pending_gets, nonce) do
      # ignore this response
      # the request has been dealt with already
      state
    else
      %{client: client, responses: responses} = state.pending_gets[nonce]
      new_responses = Map.put(responses, node, values)

      if Map.size(new_responses) >= state.r do
        Logger.info(
          "Got r or more responses for #{inspect(client)}'s get " <>
            "request [nonce=#{inspect(nonce)}]"
        )

        # enough responses, respond to client
        latest_values =
          new_responses
          |> Map.values()
          |> List.flatten()
          |> Enum.sort()
          |> Enum.dedup()
          |> remove_outdated

        send(client, %ClientResponse.Get{
          nonce: nonce,
          success: true,
          values: latest_values
        })

        # request not pending anymore, so get rid of the entry
        %{
          state
          | pending_gets: Map.delete(state.pending_gets, nonce)
        }
      else
        # not enough responses yet
        new_pending_gets =
          Map.put(state.pending_gets, nonce, %{
            client: client,
            responses: new_responses
          })

        %{state | pending_gets: new_pending_gets}
      end
    end
  end

  @doc """
  Respond to `client`'s `put` request, assuming we are
  the co-ordinator for the requested key.

  Steps:
  -- These steps happen synchronously
  1. Increment vector_clock
  2. Write to own store
  3. Send {key,value,vector_clock} to top `n` nodes in
       the preference list for key

  -- These steps happen asynchronously, i.e. outside this function
  4. Wait for responses.
  5. If (w - 1) responses received, return success,
       otherwise failure.
  """
  def put_as_coordinator(state, client, %ClientRequest.Put{
        nonce: nonce,
        key: key,
        value: value
      }) do
    state = %{
      state
      | vector_clock: VectorClock.tick(state.vector_clock, state.id)
    }

    state = put(state, key, value, state.vector_clock)

    Enum.each(get_preference_list(state, key), fn node ->
      send(node, %CoordinatorRequest.Put{
        nonce: nonce,
        key: key,
        value: value,
        clock: state.vector_clock
      })
    end)

    %{
      state
      | pending_puts:
          Map.put(state.pending_puts, nonce, %{client: client, responses: []})
    }
  end

  @doc """
  Add `key`-`value` association to local storage,
  squashing any outdated versions.
  """
  def put(state, key, value, clock) do
    Logger.debug("Writing #{inspect(value)} to key #{inspect(key)}")

    new_value = {value, clock}

    new_store =
      Map.update(state.store, key, [new_value], fn orig_values ->
        remove_outdated([new_value | orig_values])
      end)

    %{state | store: new_store}
  end

  @doc """
  Remove outdated values from a list of {value, clock} pairs.

      iex> DynamoNode.remove_outdated([])
      []

      iex> DynamoNode.remove_outdated([{:foo, %{a: 1}}, {:bar, %{a: 2}}])
      [{:bar, %{a: 2}}]

      iex> DynamoNode.remove_outdated([{:bar, %{a: 2}}, {:foo, %{a: 1}}])
      [{:bar, %{a: 2}}]

      iex> DynamoNode.remove_outdated([
      ...>   {:foo, %{a: 1, b: 9}},
      ...>   {:bar, %{a: 5, b: 1}},
      ...> ])
      [{:foo, %{a: 1, b: 9}}, {:bar, %{a: 5, b: 1}}]

      iex> DynamoNode.remove_outdated([
      ...>   {:foo, %{a: 1, b: 9}},
      ...>   {:bar, %{a: 5, b: 1}},
      ...>   {:baz, %{a: 6, b: 2}}
      ...> ])

      [{:foo, %{a: 1, b: 9}}, {:baz, %{a: 6, b: 2}}]
  """
  def remove_outdated(values) do
    List.foldr(values, [], fn {_new_val, new_clock} = new_value,
                              latest_values ->
      actual_latest_values =
        Enum.reject(latest_values, fn {_latest_val, latest_clock} ->
          VectorClock.after?(new_clock, latest_clock)
        end)

      is_new_value_latest =
        Enum.all?(actual_latest_values, fn {_latest_val, latest_clock} ->
          not VectorClock.before?(new_clock, latest_clock)
        end)

      if is_new_value_latest do
        [new_value | actual_latest_values]
      else
        actual_latest_values
      end
    end)
  end
end
