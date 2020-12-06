defmodule MeasureStatistics do
  def measure(duration) do
    # ------------------------
    # -- cluster parameters --
    # ------------------------

    cluster_size = 10
    num_keys = 100
    # common values of (n,r,w) acc. to Dynamo paper
    {n, r, w} = {3, 2, 2}

    # timeouts
    coordinator_timeout = 300
    total_redirect_timeout = 300
    request_timeout = 100
    alive_check_interval = 200
    replica_sync_interval = 500

    # requests per second
    peak_request_rate = 500

    # make up values for nodes and data
    data =
      for key <- 1..num_keys, into: %{} do
        {key, 10}
      end

    nodes =
      for node_num do
        "node-#{node_num}"
      end

    Cluster.start(
      data,
      nodes,
      n,
      r,
      w,
      coordinator_timeout,
      total_redirect_timeout,
      request_timeout,
      alive_check_interval,
      replica_sync_interval
    )

    # initialize state
    state = %{
      last_written: data,
      num_inconsistencies: 0,
      num_stale_reads: 0,
      num_requests_failed: 0,
      num_requests_succeeded: 0,
      nodes: nodes,
      contexts: [],
      pending_gets: %{},
      pending_puts: %{}
    }

    # start timer for how long we want to run this simulation
    timer(duration, :measure_finish)

    measure_loop(state)
  end

  def measure_loop(state) do
    # whenever we send a request
    # things to choose from:
    # 1. get or put request
    # 2. which node to send to
    # 3. which key
    # if put {
    #   4. which context
    # }
    # if put, then the value is chosen for us by (10 + last_written[key])
    # 5. timeout for next request

    get_or_put = Enum.random([:get, :put])
    node = Enum.random(state.nodes)
    {key, last_written_val} = Enum.random(state.last_written)
    context_idx = Enum.random(1..Enum.count(state.contexts))
    context = Enum.at(state.contexts, context_idx)

    nonce = Nonce.new()
    last_value = Map.fetch!(state.last_written, key)

    state =
      if get_or_put == :get do
        msg = %ClientRequest.Get{
          nonce: nonce,
          key: key
        }

        %{
          state
          | pending_gets:
              Map.put(state.pending_gets, nonce, %{
                expected_value: last_value,
                msg: msg,
                context_idx: context_idx
              })
        }
      else
        writing_value = last_value + 10

        msg = %ClientRequest.Get{
          nonce: nonce,
          key: key,
          value: writing_value,
          context: context
        }

        %{
          state
          | pending_puts:
              Map.put(state.pending_puts, nonce, %{
                writing_value: writing_value,
                msg: msg,
                context_idx: context_idx
              })
        }
      end

    send(node, msg)

    # go over all recvd messages
    all_recvd_msgs = recv_all_msgs_in_mailbox()

    state =
      for msg <- all_recvd_msgs, reduce: state do
        state_acc ->
          case msg do
            %ClientResponse.Get{
              nonce: nonce,
              success: success,
              values: values,
              context: context
            } ->
              {%{
                 expected_value: expected_value,
                 msg: msg,
                 context_idx: context_idx
               }, new_pending_gets} = Map.pop!(state.pending_gets, nonce)
               state_acc = %{state_acc | pending_gets: new_pending_gets}

               if success == false do
                 %{state_acc | num_requests_failed: state.num_requests_failed + 1}
               else
                inconsistency? = Enum.count(values) > 1
                # NOTE: We assume the following to NOT be a stale read:
                #   write 10
                #   write 20
                #   write 30
                #   read
                #   write 40
                #   * get read response values = [10, 30] while expecting 30
                stale_read? = Enum.all?(values, fn recvd_value ->
                  recvd_value < expected_value
                end)

                # TODO
                # %{state_acc | num_requests_succ }
               end

            %ClientResponse.Put{
              nonce: nonce,
              success: success,
              values: values,
              context: context
            } ->
              state_acc
          end
      end

    # TODO Wait for next request timeout

    # send client requests to nodes
    # receive responses
    # until you receive a timeout
    receive do
      :measure_finish -> state
    after
      0 -> measure_loop(state)
    end
  end

  @doc """
  Receive all responses except for simulation finish msg.
  """
  def recv_all_msgs_in_mailbox(accumulated) do
    receive do
      msg when msg != :measure_finish ->
        [msg | recv_all_msgs_in_mailbox()]
    after
      0 -> []
    end
  end
end
