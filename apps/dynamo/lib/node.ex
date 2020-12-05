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
    field :coordinator_timeout, pos_integer()

    # Total number of milliseconds for which a node should
    # keep trying to redirect a client request before
    # failing the client request
    field :total_redirect_timeout, pos_integer()

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
    # We also store which nodes we've requested, so that we can avoid these
    # when we retry.
    field :pending_gets, %{
      required(Nonce.t()) => %{
        client: any(),
        key: any(),
        responses: %{required(any()) => {[any()], %Context{}}},
        requested: MapSet.t(any())
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

    # pending client requests we're trying to redirect
    # to an appropriate coordinator
    field :pending_redirects, %{
      required(Nonce.t()) => %{
        client: any(),
        msg: %ClientRequest.Get{} | %ClientRequest.Put{},
        get_or_put: :get | :put
      }
    }

    # what keys have we handed off, and which contexts
    field :pending_handoffs, %{
      required(Nonce.t()) => %{
        required(any()) => %Context{}
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
        total_redirect_timeout,
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
      ring: ring,
      n: n,
      r: r,
      w: w,
      coordinator_timeout: coordinator_timeout,
      total_redirect_timeout: total_redirect_timeout,
      request_timeout: request_timeout,
      alive_check_interval: alive_check_interval,
      pending_gets: %{},
      pending_puts: %{},
      pending_redirects: %{},
      pending_handoffs: %{}
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

  def client_get_fail_msg(nonce) do
    %ClientResponse.Get{
      nonce: nonce,
      success: false,
      values: nil,
      context: nil
    }
  end

  def client_put_fail_msg(nonce) do
    %ClientResponse.Put{
      nonce: nonce,
      success: false,
      context: nil
    }
  end

  def client_fail_msg(get_or_put, nonce) do
    if get_or_put == :get do
      client_get_fail_msg(nonce)
    else
      client_put_fail_msg(nonce)
    end
  end

  @doc """
  Handle, redirect, or reply failure to an incoming client request.
  """
  def handle_client_request(state, msg, client, get_or_put) do
    coord_handler =
      if get_or_put == :get do
        &coord_handle_get_req/3
      else
        &coord_handle_put_req/3
      end

    if is_valid_coordinator(state, msg.key) do
      # handle the request as coordinator
      coord_handler.(state, client, msg)
    else
      # start a timer so we know to stop retrying redirects
      timer(
        state.total_redirect_timeout,
        {:total_redirect_timeout, msg.nonce}
      )

      # put it in pending redirects
      state = %{
        state
        | pending_redirects:
            Map.put(state.pending_redirects, msg.nonce, %{
              client: client,
              msg: msg,
              get_or_put: get_or_put
            })
      }

      # redirect the request
      redirect_or_fail_client_request(state, msg.nonce)
    end
  end

  @doc """
  Redirect, or reply failure to an incoming
  client request after a redirect failure.
  """
  @spec redirect_or_fail_client_request(%DynamoNode{}, Nonce.t()) ::
          %DynamoNode{}
  def redirect_or_fail_client_request(state, nonce) do
    %{
      client: client,
      msg: received_msg,
      get_or_put: get_or_put
    } = Map.fetch!(state.pending_redirects, nonce)

    coord = get_first_alive_coordinator(state, received_msg.key)

    cond do
      coord != nil ->
        # redirect to coordinator
        send_with_async_timeout(
          state,
          coord,
          %RedirectedClientRequest{
            client: client,
            request: received_msg
          },
          {:redirect_timeout, nonce, coord}
        )

        state

      coord == nil ->
        # no valid coordinator, reply failure
        send(client, client_fail_msg(get_or_put, nonce))

        # client request taken care of, no need to redirect anymore
        %{
          state
          | pending_redirects: Map.delete(state.pending_redirects, nonce)
        }
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
      {client, %ClientRequest.Get{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        state =
          handle_client_request(
            state,
            msg,
            client,
            :get
          )

        listener(state)

      {client, %ClientRequest.Put{} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(client)}")

        state =
          handle_client_request(
            state,
            msg,
            client,
            :put
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

        # request has been handler (in case we were trying to redirect it)
        state = %{
          state
          | pending_redirects: Map.delete(state.pending_redirects, nonce)
        }

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

        # request has been handler (in case we were trying to redirect it)
        state = %{
          state
          | pending_redirects: Map.delete(state.pending_redirects, nonce)
        }

        state = mark_alive(state, coordinator)
        state = put(state, key, [value], context)

        send(coordinator, %CoordinatorResponse.Put{
          nonce: nonce
        })

        if context.hint != nil and
             Map.get(state.nodes_alive, context.hint) == true do
          # try handing off hinted data right away
          state = handoff_hinted_data(state, context.hint)
          listener(state)
        else
          listener(state)
        end

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
         nonce: nonce,
         data: data
       } = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)
        state = put_all(state, data)

        # send acknowledgement
        send(node, %HandoffResponse{nonce: nonce})

        listener(state)

      {node, %HandoffResponse{nonce: nonce} = msg} ->
        Logger.info("Received #{inspect(msg)} from #{inspect(node)}")
        state = mark_alive(state, node)

        if not Map.has_key?(state.pending_handoffs, nonce) do
          # this handoff timed out already, so ignore this response
          listener(state)
        else
          {keys, new_pending_handoffs} = Map.pop!(state.pending_handoffs, nonce)
          state = %{state | pending_handoffs: new_pending_handoffs}
          state = delete_handed_off_keys(state, keys)
          listener(state)
        end

      # timeouts
      # client request timeouts at coordinator
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

      # coord-request timeouts
      {:coordinator_request_timeout, :get, nonce, node} = msg ->
        Logger.info("Received #{inspect(msg)}")

        req_state = Map.get(state.pending_gets, nonce)

        # either the client request has been dealt with, or we've received
        # a coord-response from this node
        retry_not_required? =
          req_state == nil or Map.has_key?(req_state.responses, node)

        if retry_not_required? do
          # ignore this timeout
          listener(state)
        else
          # otherwise, we didn't get a response for this coord-request in time
          # so assume node is dead, and try to request someone else
          state = mark_dead(state, node)

          %{key: key, requested: already_requested} = req_state

          # get first alive node we've not already requested
          all_nodes_ordered =
            HashRing.find_nodes(
              state.ring,
              key,
              map_size(state.nodes_alive) + 1
            )

          new_node =
            Enum.find(all_nodes_ordered, fn node ->
              not MapSet.member?(already_requested, node) and
                (node == state.id or Map.get(state.nodes_alive, node) == true)
            end)

          if new_node != nil do
            # request this node
            send_with_async_timeout(
              state,
              new_node,
              %CoordinatorRequest.Get{
                nonce: nonce,
                key: key
              },
              {:coordinator_request_timeout, :get, nonce, new_node}
            )

            # update state accordingly
            new_req_state = %{
              req_state
              | requested: MapSet.put(already_requested, new_node)
            }

            state = %{
              state
              | pending_gets: Map.put(state.pending_gets, nonce, new_req_state)
            }

            listener(state)
          else
            # nobody else we can request, so don't retry
            listener(state)
          end
        end

      # redirect timeouts
      {:total_redirect_timeout, nonce} = msg ->
        # time up for redirect attempts, give up now
        Logger.info("Received #{inspect(msg)}")

        case Map.get(state.pending_redirects, nonce) do
          nil ->
            # request already been handled
            listener(state)

          %{client: client, get_or_put: get_or_put} ->
            # pending for too long, just fail the request
            send(client, client_fail_msg(get_or_put, nonce))

            state = %{
              state
              | pending_redirects: Map.delete(state.pending_redirects, nonce)
            }

            listener(state)
        end

      {:redirect_timeout, nonce, failed_coord} = msg ->
        # redirect attempt failed, try again
        Logger.info("Received #{inspect(msg)}")

        if not Map.has_key?(state.pending_redirects, nonce) do
          # request already been handled successfully
          listener(state)
        else
          # retry redirecting
          state = mark_dead(state, failed_coord)
          state = redirect_or_fail_client_request(state, nonce)
          listener(state)
        end

      # handoff timeout
      {:handoff_timeout, nonce, node} = msg ->
        # consider node dead, we'll retry handoff later
        # when it comes alive
        Logger.info("Received #{inspect(msg)}")

        if Map.has_key?(state.pending_handoffs, nonce) do
          # didn't receive response before this timeout
          # so mark node dead, and remove this from pending
          state = mark_dead(state, node)

          state = %{
            state
            | pending_handoffs: Map.delete(state.pending_handoffs, nonce)
          }

          listener(state)
        else
          # already handed off, so ignore
          listener(state)
        end

      # health checks
      :alive_check_interval = msg ->
        # time to check on dead nodes' health
        Logger.debug("Received #{inspect(msg)}")

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

      # error
      unhandled_msg ->
        raise "Received unhandled msg: #{inspect(unhandled_msg)}"
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
  """
  @spec coord_handle_get_req(%DynamoNode{}, any(), %ClientRequest.Get{}) ::
          %DynamoNode{}
  def coord_handle_get_req(state, client, %ClientRequest.Get{
        nonce: nonce,
        key: key
      }) do
    alive_pref_list = get_alive_preference_list(state, key)

    Enum.each(alive_pref_list, fn node ->
      # DO send get request to self
      send_with_async_timeout(
        state,
        node,
        %CoordinatorRequest.Get{
          nonce: nonce,
          key: key
        },
        {:coordinator_request_timeout, :get, nonce, node}
      )
    end)

    # start timer for the responses
    timer(state.coordinator_timeout, {:coordinator_timeout, :get, nonce})

    %{
      state
      | pending_gets:
          Map.put(state.pending_gets, nonce, %{
            client: client,
            key: key,
            responses: %{},
            requested: MapSet.new(alive_pref_list)
          })
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

    get_alive_preference_list_with_intended(state, key)
    # don't send put request to self
    |> Enum.reject(fn {node, _hint} -> node == state.id end)
    |> Enum.each(fn {node, hint} ->
      send_with_async_timeout(
        state,
        node,
        %CoordinatorRequest.Put{
          nonce: nonce,
          key: key,
          value: value,
          context: %{context | hint: hint}
        },
        raise("TODO")
      )
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
  Add all `key`-`value` association to local storage.
  """
  @spec put_all(%DynamoNode{}, %{required(any()) => {[any()], %Context{}}}) ::
          %DynamoNode{}
  def put_all(state, data) do
    Enum.reduce(data, state, fn {key, {values, context}}, state_acc ->
      put(state_acc, key, values, context)
    end)
  end

  @spec delete_handed_off_key(%DynamoNode{}, any(), %Context{}) ::
          %DynamoNode{}
  def delete_handed_off_key(state, key, handed_off_context) do
    stored = Map.get(state.store, key)

    case stored do
      nil ->
        state

      {_values, stored_context} ->
        if Context.compare(stored_context, handed_off_context) == :after do
          # we have newer context to hand off, so don't get rid of key
          state
        else
          # we have handed off this key successfully and we don't
          # have newer version - get rid of the key
          %{state | store: Map.delete(state.store, key)}
        end
    end
  end

  @spec delete_handed_off_keys(%DynamoNode{}, %{required(any()) => %Context{}}) ::
          %DynamoNode{}
  def delete_handed_off_keys(state, keys) do
    Enum.reduce(keys, state, fn {key, context}, state_acc ->
      delete_handed_off_key(state_acc, key, context)
    end)
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
      ring: state.ring,
      n: state.n,
      r: state.r,
      w: state.w,
      coordinator_timeout: state.coordinator_timeout,
      total_redirect_timeout: state.total_redirect_timeout,
      request_timeout: state.request_timeout,
      alive_check_interval: state.alive_check_interval,
      pending_gets: %{},
      pending_puts: %{},
      pending_redirects: %{},
      pending_handoffs: %{}
    }

    crash_wait_loop()

    timer(state.alive_check_interval, :alive_check_interval)
    wiped_state
  end

  @doc """
  Send msg to proc. Also start a timer to inform us to retry.
  """
  def send_with_async_timeout(state, node, msg, _timeout_msg)
      when state.id == node do
    # don't start a timer for ourselves, since we'll always receive it
    send(node, msg)
  end

  @spec send_with_async_timeout(%DynamoNode{}, any(), any(), any()) ::
          :ok
  def send_with_async_timeout(state, node, msg, timeout_msg) do
    timer(state.request_timeout, timeout_msg)
    send(node, msg)
  end

  @doc """
  Mark a node dead in state.
  """
  def mark_dead(state, node) when state.id == node do
    state
  end

  def mark_dead(state, node) do
    %{state | nodes_alive: Map.replace!(state.nodes_alive, node, false)}
  end

  @doc """
  Mark a node alive, and do pending operations related to them (handoff).
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
    state = %{
      state
      | nodes_alive: Map.replace!(state.nodes_alive, node, true)
    }

    # do pending operations for this node
    state = handoff_hinted_data(state, node)

    state
  end

  @doc """
  Send data meant for `node` to it,
  assuming that it has come back alive.
  Don't get rid of the handoff data just yet;
  wait for the acknowledgement.
  """
  @spec handoff_hinted_data(%DynamoNode{}, any()) :: %DynamoNode{}
  def handoff_hinted_data(state, node) do
    handoff_data =
      Enum.filter(state.store, fn {_key, {_values, context}} ->
        context.hint == node
      end)

    nonce = Nonce.new()

    send_with_async_timeout(
      state,
      node,
      %HandoffRequest{
        nonce: nonce,
        data: handoff_data
      },
      {:handoff_timeout, nonce, node}
    )

    # make this request pending in state
    %{
      state
      | pending_handoffs: Map.put(state.pending_handoffs, nonce, %{})
    }
  end
end
