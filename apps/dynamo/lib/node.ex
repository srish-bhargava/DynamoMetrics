defmodule DynamoNode do
  @moduledoc """
  A replica node in a DynamoDB cluster.
  """

  use TypedStruct

  alias ExHashRing.HashRing

  # override Kernel's functions with Emulation's
  import Emulation, only: [send: 2, timer: 2, cancel_timer: 1]

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

    # Ongoing request timers for nodes this node has sent
    # messages to, so that they can be cancelled
    field :request_timers, %{required(any()) => reference()}

    # hash ring
    field :ring, ExHashRing.HashRing.t()

    # parameters from dynamo
    # for minimum participants in read/write
    field :n, pos_integer()
    field :r, pos_integer()
    field :w, pos_integer()

    # Number of milliseconds for which a coordinator
    # should wait before failing a client request
    field :coordinator_timeout, pos_integer()

    # Number of milliseconds for which a node should try
    # to redirect to a client request before failing it
    field :redirect_timeout, pos_integer()

    # Number of milliseconds a node should wait for
    # a response from another node
    field :request_timeout, pos_integer()

    # Number of milliseconds a node should wait before
    # checking on nodes it thinks are dead
    field :alive_check_interval, pos_integer()

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
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) ::
          no_return()
  def start(
        id,
        data,
        nodes,
        n,
        r,
        w,
        coordinator_timeout,
        redirect_timeout,
        request_timeout,
        alive_check_interval
      ) do
    Logger.info("Starting node #{inspect(id)}")
    Logger.metadata(id: id)

    ring = HashRing.new(nodes, 1)

    # convert data from a key-value map to a versioned store
    # only store data that you're concerned with
    # (i.e. you're in preference list for)
    store =
      data
      |> Enum.filter(fn {k, _v} ->
        id in HashRing.find_nodes(ring, k, n)
      end)
      |> Map.new(fn {k, v} ->
        {k, {[v], %Context{version: VectorClock.new()}}}
      end)

    nodes_alive =
      nodes
      |> List.delete(id)
      |> Map.new(fn node -> {node, true} end)

    state = %DynamoNode{
      id: id,
      store: store,
      nodes_alive: nodes_alive,
      request_timers: %{},
      ring: ring,
      n: n,
      r: r,
      w: w,
      coordinator_timeout: coordinator_timeout,
      redirect_timeout: redirect_timeout,
      request_timeout: request_timeout,
      alive_check_interval: alive_check_interval,
      pending_gets: %{},
      pending_puts: %{}
    }

    timer(state.alive_check_interval, :alive_check_interval)

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
  Return the first valid coordinator that you think is alive,
  nil if no valid coordinator is alive.
  """
  @spec get_first_alive_coordinator(%DynamoNode{}, any()) :: any() | nil
  def get_first_alive_coordinator(state, key) do
    pref_list = get_preference_list(state, key)

    Enum.find(pref_list, nil, fn node ->
      node == state.id or state.nodes_alive[node] == true
    end)
  end

  @doc """
  Return a list of the top `n` healthy nodes for a particular key.
  Return fewer if there are fewer than `n` nodes alive in total.
  """
  @spec get_alive_preference_list(%DynamoNode{}, any()) :: [any()]
  def get_alive_preference_list(state, key) do
    all_nodes_ordered =
      HashRing.find_nodes(state.ring, key, map_size(state.nodes_alive) + 1)

    only_healthy =
      Enum.filter(all_nodes_ordered, fn node ->
        node == state.id or state.nodes_alive[node] == true
      end)

    Enum.take(only_healthy, state.n)
  end

  @doc """
  Return a list of the top `n` healthy nodes for a particular key,
  along with the originally intended recipient (who's dead) and nil
  if it is the intended recipient.
  """
  @spec get_alive_preference_list_with_intended(%DynamoNode{}, any()) :: [
          {any(), any() | nil}
        ]
  def get_alive_preference_list_with_intended(state, key) do
    orig_pref_list = get_preference_list(state, key)
    alive_pref_list = get_alive_preference_list(state, key)

    dead_origs = orig_pref_list -- alive_pref_list

    unintendeds =
      Enum.filter(alive_pref_list, fn node -> node not in orig_pref_list end)

    hints = Map.new(Enum.zip(unintendeds, dead_origs))

    Enum.map(alive_pref_list, fn node ->
      {node, Map.get(hints, node)}
    end)
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
  Handle, redirect, or reply failure to an incoming client request.
  """
  def handle_client_request(
        state,
        received_msg,
        client,
        coord_handler,
        fail_msg
      ) do
    coord = get_first_alive_coordinator(state, received_msg.key)

    cond do
      is_valid_coordinator(state, received_msg.key) ->
        # handle the request as coordinator
        coord_handler.(state, client, received_msg)

      coord != nil ->
        # redirect to coordinator
        send_with_async_timeout(state, coord, %RedirectedClientRequest{
          client: client,
          request: received_msg
        })

      coord == nil ->
        # no valid coordinator, reply failure
        send(client, fail_msg)
        state
    end
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
      {client, %ClientRequest.Get{nonce: nonce} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        state =
          handle_client_request(
            state,
            msg,
            client,
            &coord_handle_get_req/3,
            %ClientResponse.Get{
              nonce: nonce,
              success: false,
              values: nil,
              context: nil
            }
          )

        listener(state)

      {client, %ClientRequest.Put{nonce: nonce} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        state =
          handle_client_request(
            state,
            msg,
            client,
            &coord_handle_put_req/3,
            %ClientResponse.Put{
              nonce: nonce,
              success: false,
              context: nil
            }
          )

        listener(state)

      # redirects from other nodes
      {node,
       %RedirectedClientRequest{
         client: client,
         request: %ClientRequest.Get{} = orig_msg
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)

        # we must be the coordinator for this key
        state = coord_handle_get_req(state, client, orig_msg)
        listener(state)

      {node,
       %RedirectedClientRequest{
         client: client,
         request: %ClientRequest.Put{} = orig_msg
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)

        # we must be the coordinator for this key
        state = coord_handle_put_req(state, client, orig_msg)
        listener(state)

      # coordinator requests
      {coordinator, %CoordinatorRequest.Get{nonce: nonce, key: key} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(coordinator)}")
        state = mark_alive(state, coordinator)

        stored = Map.get(state.store, key)

        case stored do
          {values, context} ->
            send(coordinator, %CoordinatorResponse.Get{
              nonce: nonce,
              values: values,
              # remove the hint, so it doesn't get stored somewhere
              # else as well
              context: %{context | hint: nil}
            })

          nil ->
            Logger.debug("Don't have key #{inspect(key)}, so not responding")
        end

        listener(state)

      {coordinator,
       %CoordinatorRequest.Put{
         nonce: nonce,
         key: key,
         value: value,
         context: context
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(coordinator)}")
        state = mark_alive(state, coordinator)
        state = put(state, key, [value], context)

        state =
          send_with_async_timeout(state, coordinator, %CoordinatorResponse.Put{
            nonce: nonce
          })

        listener(state)

      # node responses to coordinator requests
      {node, %CoordinatorResponse.Get{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        state = coord_handle_get_resp(state, node, msg)
        listener(state)

      {node, %CoordinatorResponse.Put{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        state = coord_handle_put_resp(state, node, msg)
        listener(state)

      # handoffs
      {node,
       %HandoffRequest{
         key: key,
         values: values,
         context: context
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        state = put(state, key, values, context)
        listener(state)

      # timeouts
      {:coordinator_timeout, :get, nonce} = msg ->
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

      {:coordinator_timeout, :put, nonce} = msg ->
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

      {:request_timeout, node} = msg ->
        # mark this node dead
        Logger.info("Received #{inspect(msg)}")

        state = %{
          state
          | nodes_alive: Map.replace!(state.nodes_alive, node, false)
        }

        listener(state)

      # health checks
      :alive_check_interval = msg ->
        # time to check on dead nodes' health
        Logger.info("Received #{inspect(msg)}")

        for {node, false} <- state.nodes_alive do
          # don't start a timeout on this msg since
          # we already consider these dead
          send(node, :alive_check_request)
        end

        timer(state.alive_check_interval, :alive_check_interval)
        listener(state)

      {node, :alive_check_request = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        # don't start a timeout, since we just found out it's alive
        send(node, :alive_check_response)
        listener(state)

      {node, :alive_check_response = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        listener(state)

      # testing
      {from, %TestRequest{nonce: nonce}} ->
        # respond with our current state
        send(from, %TestResponse{
          nonce: nonce,
          state: state
        })

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
  @spec coord_handle_get_req(%DynamoNode{}, any(), %ClientRequest.Get{}) ::
          %DynamoNode{}
  def coord_handle_get_req(state, client, %ClientRequest.Get{
        nonce: nonce,
        key: key
      }) do
    # since send_with_async_timeout modifies state,
    # we need to reduce over the list of nodes while calling it
    # to get the final state
    state =
      Enum.reduce(get_alive_preference_list(state, key), state, fn node,
                                                                   state_acc ->
        # DO send get request to self
        send_with_async_timeout(state_acc, node, %CoordinatorRequest.Get{
          nonce: nonce,
          key: key
        })
      end)

    # start timer for the responses
    timer(state.coordinator_timeout, {:coordinator_timeout, :get, nonce})

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
    state = put(state, key, [value], context)

    state =
      get_alive_preference_list_with_intended(state, key)
      # don't send put request to self
      |> Enum.reject(fn {node, _hint} -> node == state.id end)
      |> Enum.reduce(state, fn {node, hint}, state_acc ->
        send_with_async_timeout(state_acc, node, %CoordinatorRequest.Put{
          nonce: nonce,
          key: key,
          value: value,
          context: %{context | hint: hint}
        })
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
      timer(state.coordinator_timeout, {:coordinator_timeout, :put, nonce})

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
  @spec put(%DynamoNode{}, any(), [any()], %Context{}) :: %DynamoNode{}
  def put(state, key, values, context) do
    Logger.debug("Writing #{inspect(values)} to key #{inspect(key)}")

    new_value = {values, context}

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
    case Context.compare(context1, context2) do
      :before ->
        value2

      :after ->
        value1

      :concurrent ->
        all_vals =
          (vals1 ++ vals2)
          |> Enum.sort()
          |> Enum.dedup()

        {all_vals, Context.combine(context1, context2)}
    end
  end

  # wait for a :recover msg, ignoring all others
  defp crash_wait_loop do
    receive do
      {_from, :recover} = msg ->
        Logger.info("Received #{inspect(msg)}")

      other_msg ->
        Logger.debug("Dead, ignoring #{inspect(other_msg)}")
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
      request_timers: %{},
      ring: state.ring,
      n: state.n,
      r: state.r,
      w: state.w,
      coordinator_timeout: state.coordinator_timeout,
      redirect_timeout: state.redirect_timeout,
      request_timeout: state.request_timeout,
      alive_check_interval: state.alive_check_interval,
      pending_gets: %{},
      pending_puts: %{}
    }

    crash_wait_loop()

    timer(state.alive_check_interval, :alive_check_interval)
    wiped_state
  end

  @doc """
  Send msg to proc. Also start a timer to detect failure of proc.

  This timer is set in state.request_timers, and should be
  cancelled if a message is received from proc.

  Doesn't reset timer if it already exists.
  """
  def send_with_async_timeout(state, node, msg) when state.id == node do
    # since we deal with the weird situation where we might
    # end up sending messages to ourself (see: `coord_handle_get_req`),
    # checking that we're not keeping track of our own liveness status
    # should help us keep some small measure of sanity
    send(node, msg)
    state
  end

  @spec send_with_async_timeout(%DynamoNode{}, any(), any()) :: %DynamoNode{}
  def send_with_async_timeout(state, node, msg) do
    send(node, msg)

    %{
      state
      | request_timers:
          Map.put_new_lazy(state.request_timers, node, fn ->
            timer(state.request_timeout, {:request_timeout, node})
          end)
    }
  end

  @doc """
  Mark a node alive.

  We do this by setting its entry in state.nodes_alive to true
  and by resetting and removing its liveness timer (if it has one).
  """
  def mark_alive(state, node) when state.id == node do
    # since we deal with the weird situation where we might
    # end up sending messages to ourself (see: `coord_handle_get_req`),
    # checking that we're not keeping track of our own liveness status
    # should help us keep some small measure of sanity
    state
  end

  @spec mark_alive(%DynamoNode{}, any()) :: %DynamoNode{}
  def mark_alive(state, node) do
    {node_timer, new_request_timers} = Map.pop(state.request_timers, node)

    if node_timer != nil do
      cancel_timer(node_timer)
    end

    %{
      state
      | nodes_alive: Map.replace!(state.nodes_alive, node, true),
        request_timers: new_request_timers
    }
  end

  @doc """
  Send data meant for `node` to it, assuming that it has come back alive.
  Get rid of the corresponding entries in our own store.
  """
  @spec handoff_hinted_data(%DynamoNode{}, any()) :: %DynamoNode{}
  def handoff_hinted_data(state, node) do
    handoff_data =
      Enum.filter(state.store, fn {_key, {_values, context}} ->
        context.hint == node
      end)

    Enum.reduce(handoff_data, state, fn {key, {values, context}}, state_acc ->
      send_with_async_timeout(state_acc, node, %HandoffRequest{
        nonce: Nonce.new(),
        key: key,
        values: values,
        context: context
      })
    end)

    {_, handoff_removed_store} =
      Map.split(state.store, Enum.map(handoff_data, fn {key, _} -> key end))

    %{state | store: handoff_removed_store}
  end
end
