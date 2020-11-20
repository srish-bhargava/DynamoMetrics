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
    # %{client_nonce => %{client: client, responses: MapSet(node)}}
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
          state = coord_handle_get_req(state, client, msg)
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
          state = coord_handle_put_req(state, client, msg)
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
        state = coord_handle_get_req(state, client, orig_msg)
        listener(state)

      {node,
       %RedirectedClientRequest{
         client: client,
         request: %ClientRequest.Put{} = orig_msg
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        # we must be the coordinator for this key
        state = coord_handle_put_req(state, client, orig_msg)
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
        state = coord_handle_get_resp(state, node, msg)
        listener(state)

      {node, %CoordinatorResponse.Put{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")

        state = coord_handle_put_resp(state, node, msg)
        listener(state)
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

  -- These steps happen asynchronously in `coord_handle_get_resp`
     whenever we receive a CoordinatorResponse.Get msg
  2. Wait for r responses.
  3. Return all latest concurrent versions of the key's values
       received.

  TODO Why not handle this in the same way as put's -
       i.e. read from own store and wait for `r - 1` responses

  TODO Return failure to client on a timeout?
  """
  def coord_handle_get_req(state, client, %ClientRequest.Get{
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
  def coord_handle_get_resp(state, node, %CoordinatorResponse.Get{
        nonce: nonce,
        values: values
      }) do
    old_req_state = Map.get(state.pending_gets, nonce)

    new_req_state =
      if old_req_state == nil do
        nil
      else
        %{
          old_req_state
          | responses: Map.put(old_req_state.responses, node, values)
        }
      end

    cond do
      new_req_state == nil ->
        # ignore this response
        # the request has been dealt with already
        state

      map_size(new_req_state.responses) >= state.r ->
        Logger.info(
          "Got r or more responses for " <>
            "#{inspect(new_req_state.client)}'s get " <>
            "request [nonce=#{inspect(nonce)}]"
        )

        # enough responses, respond to client
        latest_values =
          new_req_state.responses
          |> Map.values()
          |> List.flatten()
          |> Enum.sort()
          |> Enum.dedup()
          |> VectorClock.remove_outdated()

        send(new_req_state.client, %ClientResponse.Get{
          nonce: nonce,
          success: true,
          values: latest_values
        })

        # request not pending anymore, so get rid of the entry
        %{
          state
          | pending_gets: Map.delete(state.pending_gets, nonce)
        }

      true ->
        # not enough responses yet
        %{
          state
          | pending_gets: Map.put(state.pending_gets, nonce, new_req_state)
        }
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

  TODO Return failure to client on a timeout?
  """
  def coord_handle_put_req(state, client, %ClientRequest.Put{
        nonce: nonce,
        key: key,
        value: value
      }) do
    state = %{
      state
      | vector_clock: VectorClock.tick(state.vector_clock, state.id)
    }

    # write to own store
    state = put(state, key, value, state.vector_clock)

    Enum.each(get_preference_list(state, key), fn node ->
      # don't send put request to self
      if node != state.id do
        send(node, %CoordinatorRequest.Put{
          nonce: nonce,
          key: key,
          value: value,
          clock: state.vector_clock
        })
      end
    end)

    if state.w <= 1 do
      # we've already written once, so this is enough
      # respond to client, and don't mark this request as pending
      send(client, %ClientResponse.Put{
        nonce: nonce,
        success: true
      })

      state
    else
      # otherwise, mark pending
      %{
        state
        | pending_puts:
            Map.put(state.pending_puts, nonce, %{
              client: client,
              responses: MapSet.new()
            })
      }
    end
  end

  @doc """
  Process a CoordinatorResponse.Put msg

  Add it to the list of responses in `pending_puts`.
  If we have `w - 1` or more responses for the corresponding client request,
  remove this request from `pending_puts` and return all latest values to
  the client.
  """
  def coord_handle_put_resp(state, node, %CoordinatorResponse.Put{
        nonce: nonce
      }) do
    old_req_state = Map.get(state.pending_gets, nonce)

    new_req_state =
      if old_req_state == nil do
        nil
      else
        %{
          old_req_state
          | responses: MapSet.put(old_req_state.responses, node)
        }
      end

    cond do
      new_req_state == nil ->
        # ignore this response
        # the request has been dealt with already
        state

      MapSet.size(new_req_state.responses) >= state.w - 1 ->
        Logger.info(
          "Got w - 1 or more responses for " <>
            "#{inspect(new_req_state.client)}'s get " <>
            "request [nonce=#{inspect(nonce)}]"
        )

        # enough responses, respond to client
        send(new_req_state.client, %ClientResponse.Put{
          nonce: nonce,
          success: true
        })

        # request not pending anymore, so get rid of the entry
        %{
          state
          | pending_puts: Map.delete(state.pending_puts, nonce)
        }

      true ->
        # not enough responses yet
        %{
          state
          | pending_puts: Map.put(state.pending_puts, nonce, new_req_state)
        }
    end
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
        VectorClock.remove_outdated([new_value | orig_values])
      end)

    %{state | store: new_store}
  end
end
