defmodule DynamoNode do
  @moduledoc """
  A replica node in a DynamoDB cluster.
  """

  use TypedStruct

  alias ExHashRing.HashRing

  # override Kernel's functions with Emulation's
  import Emulation, only: [send: 2, timer: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  # The state of each node
  typedstruct enforce: true do
    # node id
    field :id, any()

    # local storage of key-([value], context) pairs
    # only stores concurrent versions
    field :store, %{required(any()) => {[any()], %Context{}}}

    # perceived liveness status of all nodes in the cluster
    # (except the current node)
    # i.e. is this node alive or (transiently) dead?
    field :nodes_alive, %{required(any()) => boolean()}

    # hash ring
    field :ring, ExHashRing.HashRing.t()

    # parameters from dynamo
    # for minimum participants in read/write
    field :n, pos_integer()
    field :r, pos_integer()
    field :w, pos_integer()

    # Number of milliseconds for which a coordinator
    # should wait before failing a client request
    field :client_timeout, pos_integer()

    # pending client get requests being handled by this node as coordinator.
    # Importantly, if a request has been dispatched to other nodes
    # but no coordinator responses have been received yet, then the
    # appropriate client_nonce WILL BE PRESENT and will map to {client, %{}}.
    # This helps distinguish this case with the case when an enough
    # responses have already been received and client_nonce purged
    # from this map.
    field :pending_gets, %{
      required(Nonce.t()) => %{
        client: any(),
        responses: %{required(any()) => {[any()], %Context{}}}
      }
    }

    # pending client put requests being handled
    # by this node as coordinator.
    # Similar to `pending_gets`, a request that's pending but for which
    # no coordinator responses have been received yet WILL have its
    # client_nonce be present and map to
    # %{client: client, context: context, responses: []}
    field :pending_puts, %{
      required(Nonce.t()) => %{
        client: any(),
        context: %Context{},
        responses: MapSet.t(any())
      }
    }
  end

  @doc """
  Set up node and start serving requests.
  """
  @spec start(
          any(),
          map(),
          [any()],
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) ::
          no_return()
  def start(id, data, nodes, n, r, w, client_timeout) do
    Logger.info("Starting node #{inspect(id)}")
    Logger.metadata(id: id)

    # convert data from a key-value map to a versioned store
    store =
      Map.new(data, fn {k, v} ->
        {k, {[v], %Context{version: VectorClock.new()}}}
      end)

    nodes_alive =
      nodes
      |> Enum.reject(fn node -> node == id end)
      |> Map.new(fn node -> {node, true} end)

    state = %DynamoNode{
      id: id,
      store: store,
      nodes_alive: nodes_alive,
      ring: HashRing.new(nodes, 1),
      n: n,
      r: r,
      w: w,
      client_timeout: client_timeout,
      pending_gets: %{},
      pending_puts: %{}
    }

    listener(state)
  end

  @doc """
  Get the preference list for a particular key
  (i.e. the top `n` nodes in the ring for this key).
  """
  @spec get_preference_list(%DynamoNode{}, any()) :: [any()]
  def get_preference_list(state, key) do
    HashRing.find_nodes(state.ring, key, state.n)
  end

  @doc """
  Check if this node is a valid coordinator for a particular key
  (i.e. in the top `n` for this key).
  """
  @spec is_valid_coordinator(%DynamoNode{}, any()) :: any()
  def is_valid_coordinator(state, key) do
    Enum.member?(get_preference_list(state, key), state.id)
  end

  @doc """
  Listen and serve requests, forever.
  """
  @spec listener(%DynamoNode{}) :: no_return()
  def listener(state) do
    receive do
      # crash
      {_from, :crash} = msg ->
        Logger.info("Received #{inspect(msg)}")
        state = crash(state)
        listener(state)

      # client requests
      {client, %ClientRequest.Get{key: key} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        if not is_valid_coordinator(state, key) do
          # we are not the coordinator, so redirect to them
          send(
            Enum.at(get_preference_list(state, key), 0),
            %RedirectedClientRequest{
              client: client,
              request: msg
            }
          )

          listener(state)
        else
          # we are the coordinator, so process the request
          state = coord_handle_get_req(state, client, msg)
          listener(state)
        end

      {client, %ClientRequest.Put{key: key} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        if not is_valid_coordinator(state, key) do
          # we are not the coordinator, so redirect to them
          send(
            Enum.at(get_preference_list(state, key), 0),
            %RedirectedClientRequest{
              client: client,
              request: msg
            }
          )

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

        {values, context} = Map.get(state.store, key)

        send(coordinator, %CoordinatorResponse.Get{
          nonce: nonce,
          values: values,
          context: context
        })

        listener(state)

      {coordinator,
       %CoordinatorRequest.Put{
         nonce: nonce,
         key: key,
         value: value,
         context: context
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(coordinator)}")

        state = put(state, key, value, context)
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

      # timeouts
      {:client_timeout, :get, nonce} = msg ->
        Logger.info("Received #{inspect(msg)}")
        req_state = Map.get(state.pending_gets, nonce)

        if req_state == nil do
          # request has already been dealt with, ignore
          listener(state)
        else
          # request timed out, get rid of the pending entry
          # and respond failure to client
          send(req_state.client, %ClientResponse.Get{
            nonce: nonce,
            success: false,
            values: nil,
            context: nil
          })

          listener(%{
            state
            | pending_gets: Map.delete(state.pending_gets, nonce)
          })
        end

      {:client_timeout, :put, nonce} = msg ->
        Logger.info("Received #{inspect(msg)}")
        req_state = Map.get(state.pending_puts, nonce)

        if req_state == nil do
          # request has already been dealt with, ignore
          listener(state)
        else
          # request timed out, get rid of the pending entry
          # and respond failure to client
          send(req_state.client, %ClientResponse.Put{
            nonce: nonce,
            success: false,
            context: nil
          })

          listener(%{
            state
            | pending_puts: Map.delete(state.pending_puts, nonce)
          })
        end
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
  @spec coord_handle_get_req(%DynamoNode{}, any(), %ClientRequest.Get{}) ::
          %DynamoNode{}
  def coord_handle_get_req(state, client, %ClientRequest.Get{
        nonce: nonce,
        key: key
      }) do
    Enum.each(get_preference_list(state, key), fn node ->
      # DO send get request to self
      send(node, %CoordinatorRequest.Get{nonce: nonce, key: key})
    end)

    # start timer for the responses
    timer(state.client_timeout, {:client_timeout, :get, nonce})

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
  @spec coord_handle_get_resp(%DynamoNode{}, any(), %CoordinatorResponse.Get{}) ::
          %DynamoNode{}
  def coord_handle_get_resp(state, node, %CoordinatorResponse.Get{
        nonce: nonce,
        values: values,
        context: context
      }) do
    old_req_state = Map.get(state.pending_gets, nonce)

    new_req_state =
      if old_req_state == nil do
        nil
      else
        %{
          old_req_state
          | responses: Map.put(old_req_state.responses, node, {values, context})
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
        {latest_values, context} =
          new_req_state.responses
          |> Map.values()
          |> Enum.reduce(&merge_values/2)

        send(new_req_state.client, %ClientResponse.Get{
          nonce: nonce,
          success: true,
          values: latest_values,
          context: context
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
  1. Increment vector_clock of request context for own id
  2. Write to own store
  3. Send {key,value,vector_clock} to top `n` nodes in
       the preference list for key

  -- These steps happen asynchronously, i.e. outside this function
  4. Wait for responses.
  5. If (w - 1) responses received, return success,
       otherwise failure.

  TODO Return failure to client on a timeout?
  """
  @spec coord_handle_put_req(%DynamoNode{}, any(), %ClientRequest.Put{}) ::
          %DynamoNode{}
  def coord_handle_put_req(state, client, %ClientRequest.Put{
        nonce: nonce,
        key: key,
        value: value,
        context: context
      }) do
    context = %{context | version: VectorClock.tick(context.version, state.id)}

    # write to own store
    state = put(state, key, value, context)

    Enum.each(get_preference_list(state, key), fn node ->
      # don't send put request to self
      if node != state.id do
        send(node, %CoordinatorRequest.Put{
          nonce: nonce,
          key: key,
          value: value,
          context: context
        })
      end
    end)

    if state.w <= 1 do
      # we've already written once, so this is enough
      # respond to client, and don't mark this request as pending
      send(client, %ClientResponse.Put{
        nonce: nonce,
        success: true,
        context: context
      })

      state
    else
      # otherwise, start timer for the responses and mark pending
      timer(state.client_timeout, {:client_timeout, :put, nonce})

      %{
        state
        | pending_puts:
            Map.put(state.pending_puts, nonce, %{
              client: client,
              context: context,
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
  @spec coord_handle_put_resp(%DynamoNode{}, any(), %CoordinatorResponse.Put{}) ::
          %DynamoNode{}
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
          success: true,
          context: new_req_state.context
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
  @spec put(%DynamoNode{}, any(), any(), %Context{}) :: %DynamoNode{}
  def put(state, key, value, context) do
    Logger.debug("Writing #{inspect(value)} to key #{inspect(key)}")

    new_value = {[value], context}

    new_store =
      Map.update(state.store, key, new_value, fn orig_value ->
        merge_values(new_value, orig_value)
      end)

    %{state | store: new_store}
  end

  @doc """
  Utility function to remove outdated values from a list of {value, clock} pairs.
  """
  @spec merge_values({[any()], %Context{}}, {[any()], %Context{}}) ::
          {[any()], %Context{}}
  def merge_values({vals1, context1} = value1, {vals2, context2} = value2) do
    case VectorClock.compare(context1.version, context2.version) do
      :before ->
        value2

      :after ->
        value1

      :concurrent ->
        all_vals =
          (vals1 ++ vals2)
          |> Enum.sort()
          |> Enum.dedup()

        {all_vals,
         %Context{
           version: VectorClock.combine(context1.version, context2.version)
         }}
    end
  end

  # wait for a :recover msg, ignoring all others
  defp crash_wait_loop do
    receive do
      {_from, :recover} = msg ->
        Logger.info("Received #{inspect(msg)}")

      other_msg ->
        Logger.info("Dead, ignoring #{inspect(other_msg)}")
        crash_wait_loop()
    end
  end

  @doc """
  Simulate a node crash.
  Wipe transient data and wait for a :recover message.
  """
  @spec crash(%DynamoNode{}) :: %DynamoNode{}
  def crash(state) do
    wiped_state = %DynamoNode{
      id: state.id,
      store: state.store,
      nodes_alive:
        Map.new(state.nodes_alive, fn {node, _alive} -> {node, true} end),
      ring: state.ring,
      n: state.n,
      r: state.r,
      w: state.w,
      client_timeout: state.client_timeout,
      pending_gets: %{},
      pending_puts: %{}
    }

    crash_wait_loop()

    wiped_state
  end
end
