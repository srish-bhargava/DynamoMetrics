defmodule Params do
  use TypedStruct

  typedstruct do
    field :duration, pos_integer, enforce: true
    field :request_rate, {pos_integer, pos_integer}, enforce: true
    field :drop_rate, pos_integer, enforce: true
    field :mean_delay, pos_integer, enforce: true
    field :tt_fail, pos_integer, enforce: true
    field :tt_recover, pos_integer, default: 1000

    field :cluster_size, pos_integer, default: 100
    field :num_keys, pos_integer, default: 100
    field :num_clients, pos_integer, default: 5

    field :n, pos_integer, default: 3
    field :r, pos_integer, default: 2
    field :w, pos_integer, default: 2

    field :coordinator_timeout, pos_integer, default: 300
    field :total_redirect_timeout, pos_integer, default: 300
    field :request_timeout, pos_integer, default: 700
    field :alive_check_interval, pos_integer, default: 200
    field :replica_sync_interval, pos_integer, default: 500
  end
end

defmodule MeasureStatistics do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Logger

  def measure(params) do
    Emulation.init()

    Emulation.append_fuzzers([
      Fuzzers.delay(params.mean_delay / 1000),
      Fuzzers.drop(params.drop_rate)
    ])

    {:ok, crash_fuzzer} = CrashFuzzer.start()

    # make up values based on params
    data =
      for key <- 1..params.num_keys, into: %{} do
        {key, 10}
      end

    nodes =
      for node_num <- 1..params.cluster_size, into: [] do
        "node-#{node_num}"
      end

    contexts =
      for _client <- 1..params.num_clients, into: [] do
        %Context{version: VectorClock.new()}
      end

    {min_request_rate, max_request_rate} = params.request_rate
    min_request_interval = ceil(1000 / min_request_rate)
    max_request_interval = floor(1000 / max_request_rate)

    pids =
      Cluster.start(
        data,
        nodes,
        params.n,
        params.r,
        params.w,
        params.coordinator_timeout,
        params.total_redirect_timeout,
        params.request_timeout,
        params.alive_check_interval,
        params.replica_sync_interval
      )

    CrashFuzzer.enable(
      crash_fuzzer,
      params.tt_fail,
      params.tt_recover,
      pids
    )

    # initialize state
    state = %{
      last_written: data,
      num_inconsistencies: 0,
      num_stale_reads: 0,
      num_requests_failed: 0,
      num_requests_succeeded: 0,
      nodes: nodes,
      contexts: contexts,
      pending_gets: %{},
      pending_puts: %{},
      min_request_interval: min_request_interval,
      max_request_interval: max_request_interval
    }

    # start timer for how long we want to run this simulation
    # We cannot use Emulation.timer here here because this
    # process has not been spawned by Emulation,
    # so we use Process.send_after instead
    Process.send_after(self(), :measure_finish, params.duration)

    state = measure_loop(state)

    Emulation.terminate()

    # calculate stats
    total_requests = state.num_requests_failed + state.num_requests_succeeded

    availability_percent =
      Float.round(state.num_requests_succeeded * 100 / total_requests, 2)

    inconsistency_percent =
      Float.round(state.num_inconsistencies * 100 / total_requests, 2)

    stale_reads_percent =
      Float.round(state.num_stale_reads * 100 / total_requests, 4)

    Logger.flush()

    IO.puts("\n\n\n")
    IO.puts("----------------------------")
    IO.puts("    Measurements finished   ")
    IO.puts("----------------------------")
    IO.puts("Duration:        #{params.duration / 1000} s")
    IO.puts("Request rate:    #{min_request_rate}-#{max_request_rate}/s")
    IO.puts("Drop rate:       #{params.drop_rate * 100}%")
    IO.puts("Mean delay:      #{params.mean_delay / 1000} s")
    IO.puts("Mean TT fail:    #{params.tt_fail / 1000} s")
    IO.puts("Mean TT recover: #{params.tt_recover / 1000} s")
    IO.puts("----------------------------")
    IO.puts("Total requests:  #{total_requests}")
    IO.puts("Availability:    #{availability_percent}%")
    IO.puts("Inconsistencies: #{inconsistency_percent}%")
    IO.puts("Stale reads:     #{stale_reads_percent}%")
  end

  def measure_loop(state) do
    state = send_random_client_request(state)
    state = handle_all_recvd_msgs(state)
    wait_before_next_request(state)

    if finished?() do
      state
    else
      measure_loop(state)
    end
  end

  def create_random_client_request(state) do
    # things to choose from:
    # 1. get or put request
    # 2. which node to send to
    # 3. which key
    # if put {
    #   4. which context
    # }
    # if put, then the value is chosen for us by (10 + last_written[key])
    #
    # We pick from all of these randomly.

    get_or_put = Enum.random([:get, :put])
    {key, last_value} = Enum.random(state.last_written)
    context_idx = Enum.random(1..Enum.count(state.contexts)) - 1
    context = Enum.at(state.contexts, context_idx)

    nonce = Nonce.new()

    if Map.has_key?(state.pending_gets, nonce) or
         Map.has_key?(state.pending_puts, nonce) do
      # This should almost never happen
      # In case it does, just try agian
      raise "Duplicate nonce!"
    end

    msg =
      case get_or_put do
        :get ->
          %ClientRequest.Get{
            nonce: nonce,
            key: key
          }

        :put ->
          %ClientRequest.Put{
            nonce: nonce,
            key: key,
            value: last_value + 10,
            context: context
          }
      end

    {msg, context_idx}
  end

  def send_random_client_request(state) do
    node = Enum.random(state.nodes)
    {msg, context_idx} = create_random_client_request(state)

    Logger.warn("Sending: #{inspect(msg, pretty: true)} to #{inspect(node)}")
    send(node, msg)

    case msg do
      %ClientRequest.Get{nonce: nonce, key: key} ->
        %{
          state
          | pending_gets:
              Map.put(state.pending_gets, nonce, %{
                expected_value: Map.fetch!(state.last_written, key),
                msg: msg,
                context_idx: context_idx
              })
        }

      %ClientRequest.Put{nonce: nonce} ->
        %{
          state
          | pending_puts:
              Map.put(state.pending_puts, nonce, %{
                msg: msg,
                context_idx: context_idx
              })
        }
    end
  end

  def handle_recvd_msg(state, msg) do
    pending_map =
      case msg do
        %ClientResponse.Get{} -> state.pending_gets
        %ClientResponse.Put{} -> state.pending_puts
      end

    if not Map.has_key?(pending_map, msg.nonce) do
      # we're receiving a duplicate response, ignore
      state
    else
      case msg do
        %ClientResponse.Get{
          nonce: nonce,
          success: success,
          values: values,
          context: context
        } ->
          {%{
             expected_value: expected_value,
             msg: _msg,
             context_idx: context_idx
           }, new_pending_gets} = Map.pop!(state.pending_gets, nonce)

          state = %{state | pending_gets: new_pending_gets}

          if success == false do
            %{
              state
              | num_requests_failed: state.num_requests_failed + 1
            }
          else
            # update context at context_idx
            updated_contexts =
              List.update_at(state.contexts, context_idx, fn _ctx ->
                context
              end)

            inconsistency? = Enum.count(values) > 1
            # NOTE: We assume the following to NOT be a stale read:
            #   write 10
            #   write 20
            #   write 30
            #   read
            #   write 40
            #   * get read response values = [10, 40] while expecting 30
            stale_read? =
              Enum.all?(
                values,
                fn recvd_value -> recvd_value < expected_value end
              )

            %{
              state
              | num_requests_succeeded: state.num_requests_succeeded + 1,
                num_inconsistencies:
                  state.num_inconsistencies + if(inconsistency?, do: 1, else: 0),
                num_stale_reads:
                  state.num_stale_reads + if(stale_read?, do: 1, else: 0),
                contexts: updated_contexts
            }
          end

        %ClientResponse.Put{
          nonce: nonce,
          success: success,
          values: resp_values,
          context: context
        } ->
          {%{
             msg: msg,
             context_idx: context_idx
           }, new_pending_puts} = Map.pop!(state.pending_puts, nonce)

          state = %{state | pending_puts: new_pending_puts}

          if success == false do
            %{
              state
              | num_requests_failed: state.num_requests_failed + 1
            }
          else
            # update context at context_idx
            updated_contexts =
              List.update_at(state.contexts, context_idx, fn _ctx ->
                context
              end)

            # potentially update last_written
            update_last_written =
              if msg.value in resp_values do
                # if the version we sent is concurrent with the version we got back
                # (or even *after*,but that should not be possible)
                # then we know the value has been persisted
                Map.update!(
                  state.last_written,
                  msg.key,
                  &max(msg.value, &1)
                )
              else
                state.last_written
              end

            %{
              state
              | num_requests_succeeded: state.num_requests_succeeded + 1,
                contexts: updated_contexts,
                last_written: update_last_written
            }
          end
      end
    end
  end

  def handle_all_recvd_msgs(state) do
    # go over all recvd messages
    all_recvd_msgs = recv_all_msgs_in_mailbox()
    Logger.warn("#{Enum.count(all_recvd_msgs)} messages received")

    for msg <- all_recvd_msgs, reduce: state do
      state_acc ->
        Logger.warn("Received: #{inspect(msg, pretty: true)}")
        handle_recvd_msg(state_acc, msg)
    end
  end

  def wait_before_next_request(state) do
    # Wait for next request timeout
    next_request_timeout =
      Enum.random(state.min_request_interval..state.max_request_interval)

    receive do
    after
      next_request_timeout -> true
    end
  end

  def finished?() do
    receive do
      :measure_finish -> true
    after
      0 -> false
    end
  end

  @doc """
  Receive all responses except for simulation finish msg.
  """
  def recv_all_msgs_in_mailbox() do
    receive do
      msg when msg != :measure_finish ->
        [msg | recv_all_msgs_in_mailbox()]
    after
      0 -> []
    end
  end
end
