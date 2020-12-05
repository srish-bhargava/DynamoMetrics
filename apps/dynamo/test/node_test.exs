defmodule DynamoNodeTest do
  use ExUnit.Case
  doctest DynamoNode

  require Logger

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias ExHashRing.HashRing

  setup do
    Emulation.init()
    # NOTE Ideally, we'd run `Emulation.terminate()` after the test has ended
    # However, the way `ExUnit` is set up does not allow this without repeating
    # `Emulation.terminate()` in each test, since on_exit callbacks are run
    # in a *separate* process after the test process ends.
    # The fact that these test processes end quickly gives me hope that the
    # relevant teardown is unnecesary, but I might be very mistaken.
    #
    # TLDR: not calling Emulation.terminate() makes me uneasy, but not enough
    #       to do it in every test.

    # register own name so we can use it in a list of nodes
    # (needs to implement String.chars, and pid doesn't)
    Process.register(self(), :test_proc)

    :ok
  end

  # Wait for timeout milliseconds
  defp wait(timeout) do
    receive do
    after
      timeout -> true
    end
  end

  defp new_context() do
    %Context{version: VectorClock.new()}
  end

  describe "No crashes" do
    test "during startup of a single node" do
      handle =
        Process.monitor(
          spawn(:node, fn ->
            DynamoNode.start(
              :node,
              %{},
              [:node],
              1,
              1,
              1,
              1_000,
              1_000,
              9999,
              500,
              700
            )
          end)
        )

      receive do
        {:DOWN, ^handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        2_000 ->
          true
      end
    end

    test "during startup of multiple nodes" do
      nodes = [:a, :b, :c]

      for node <- nodes do
        Process.monitor(
          spawn(node, fn ->
            DynamoNode.start(node, %{}, nodes, 1, 1, 1, 1_000, 1_000, 9999, 500, 700)
          end)
        )
      end

      receive do
        {:DOWN, _handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        2_000 ->
          true
      end
    end

    test "on a get request" do
      nodes = [:a, :b, :c]

      for node <- nodes do
        Process.monitor(
          spawn(node, fn ->
            DynamoNode.start(
              node,
              %{foo: 42},
              nodes,
              1,
              1,
              1,
              1_000,
              1_000,
              9999,
              500,
              700
            )
          end)
        )
      end

      send(:a, %ClientRequest.Get{nonce: Nonce.new(), key: :foo})

      receive do
        {:DOWN, _handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        2_000 ->
          true
      end
    end

    test "on a put request" do
      nodes = [:a, :b, :c]

      for node <- nodes do
        Process.monitor(
          spawn(node, fn ->
            DynamoNode.start(
              node,
              %{foo: 42},
              nodes,
              1,
              1,
              1,
              1_000,
              1_000,
              9999,
              500,
              700
            )
          end)
        )
      end

      send(:a, %ClientRequest.Put{
        nonce: Nonce.new(),
        key: :foo,
        value: 49,
        context: new_context()
      })

      receive do
        {:DOWN, _handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        2_000 ->
          true
      end
    end
  end

  test "First get request returns the initial value" do
    Cluster.start(%{foo: 42}, [:a, :b, :c], 3, 2, 2, 1_000, 1_000, 9999, 500, 700)

    nonce = Nonce.new()
    send(:a, %ClientRequest.Get{nonce: nonce, key: :foo})

    assert_receive %ClientResponse.Get{
                     nonce: ^nonce,
                     success: true,
                     values: [42],
                     context: _context
                   },
                   5_000
  end

  test "Simple put request is successful" do
    Cluster.start(%{}, [:a, :b, :c], 1, 1, 1, 1_000, 1_000, 9999, 500, 700)
    nonce = Nonce.new()

    send(:a, %ClientRequest.Put{
      nonce: nonce,
      key: :foo,
      value: 42,
      context: new_context()
    })

    assert_receive %ClientResponse.Put{nonce: ^nonce, success: true},
                   5_000
  end

  test "get after a put returns the put value with empty initial data" do
    Cluster.start(%{}, [:a, :b, :c], 1, 1, 1, 1_000, 1_000, 9999, 500, 700)

    nonce_put = Nonce.new()
    nonce_get = Nonce.new()

    send(:a, %ClientRequest.Put{
      nonce: nonce_put,
      key: :foo,
      value: 42,
      context: new_context()
    })

    send(:a, %ClientRequest.Get{nonce: nonce_get, key: :foo})

    assert_receive %ClientResponse.Get{
                     nonce: ^nonce_get,
                     success: true,
                     values: [42],
                     context: _context
                   },
                   5_000
  end

  test "put request overwrites key in initial data" do
    Cluster.start(%{foo: 37}, [:a, :b, :c], 1, 1, 1, 1_000, 1_000, 9999, 500, 700)

    nonce_put = Nonce.new()
    nonce_get = Nonce.new()

    send(:a, %ClientRequest.Put{
      nonce: nonce_put,
      key: :foo,
      value: 42,
      context: new_context()
    })

    send(:a, %ClientRequest.Get{nonce: nonce_get, key: :foo})

    assert_receive %ClientResponse.Get{
                     nonce: ^nonce_get,
                     success: true,
                     values: [42],
                     context: _context
                   },
                   5_000
  end

  test "Simple get with multiple nodes" do
    data = %{foo: 39, bar: 42, baz: 47}

    nodes = [:a, :b, :c, :d, :e, :f]

    Cluster.start(data, nodes, 4, 3, 2, 1_000, 1_000, 9999, 500, 700)

    for {key, value} <- data do
      nonce = Nonce.new()

      send(:c, %ClientRequest.Get{nonce: nonce, key: key})

      assert_receive %ClientResponse.Get{
                       nonce: ^nonce,
                       success: true,
                       values: [^value],
                       context: _context
                     },
                     5_000
    end
  end

  test "Simple put and get with multiple nodes" do
    data = %{foo: 39, bar: 42, baz: 47}

    nodes = [:a, :b, :c, :d, :e, :f]

    Cluster.start(data, nodes, 4, 3, 2, 1_000, 1_000, 9999, 500, 700)

    for {key, _value} <- data do
      nonce = Nonce.new()

      send(:c, %ClientRequest.Put{
        nonce: nonce,
        key: key,
        value: 100,
        context: new_context()
      })
    end

    # wait a bit for the system to settle
    wait(1_000)

    for key <- Map.keys(data) do
      nonce = Nonce.new()

      send(:e, %ClientRequest.Get{nonce: nonce, key: key})

      assert_receive %ClientResponse.Get{
                       nonce: ^nonce,
                       success: true,
                       values: [100],
                       context: _context
                     },
                     5_000
    end
  end

  test "Client get request times out after a while when sent to coordinator" do
    # make sure the node we send to is a valid coordinator
    # by making everyone a valid coordinator
    spawn(:a, fn ->
      DynamoNode.start(
        :a,
        %{foo: 42},
        [:a, :b, :c],
        3,
        3,
        3,
        500,
        1_000,
        9999,
        500,
        700
      )
    end)

    nonce = Nonce.new()
    send(:a, %ClientRequest.Get{nonce: nonce, key: :foo})

    assert_receive %ClientResponse.Get{
                     nonce: ^nonce,
                     success: false,
                     values: nil,
                     context: nil
                   },
                   1_000
  end

  test "Client put request times out after a while when sent to coordinator" do
    # make sure the node we send to is a valid coordinator
    # by making everyone a valid coordinator
    spawn(:a, fn ->
      DynamoNode.start(
        :a,
        %{foo: 42},
        [:a, :b, :c],
        3,
        3,
        3,
        500,
        1_000,
        9999,
        500,
        700
      )
    end)

    nonce = Nonce.new()

    send(:a, %ClientRequest.Put{
      nonce: nonce,
      key: :foo,
      value: 49,
      context: new_context()
    })

    assert_receive %ClientResponse.Put{
                     nonce: ^nonce,
                     success: false,
                     context: nil
                   },
                   1_000
  end

  test "Crashed node doesn't respond to anything" do
    data = Map.new(1..1000, fn k -> {k, k} end)

    spawn(:a, fn ->
      DynamoNode.start(
        :a,
        data,
        [:a, :test_proc],
        1,
        1,
        1,
        500,
        1_000,
        9999,
        500,
        700
      )
    end)

    send(:a, :crash)

    Enum.each(data, fn {key, _value} ->
      # try sending every message
      send(:a, %ClientRequest.Get{nonce: Nonce.new(), key: key})

      send(:a, %ClientRequest.Put{
        nonce: Nonce.new(),
        key: key,
        value: :new,
        context: %Context{version: %{a: 999}}
      })

      send(:a, %CoordinatorRequest.Get{
        nonce: Nonce.new(),
        key: key
      })

      send(:a, %CoordinatorRequest.Put{
        nonce: Nonce.new(),
        key: key,
        value: :new,
        context: %Context{version: %{a: 999}}
      })

      send(:a, %CoordinatorResponse.Get{
        nonce: Nonce.new(),
        values: [:new_1, :new_2],
        context: %Context{version: %{a: 999}}
      })

      send(:a, %CoordinatorResponse.Put{
        nonce: Nonce.new()
      })
    end)

    refute_receive msg,
                   1_000,
                   "Received response (#{inspect(msg)}) for some request"
  end

  test "Node doesn't respond after recovery to msgs received during crash" do
    data = Map.new(1..10, fn k -> {k, k} end)

    spawn(:a, fn ->
      DynamoNode.start(
        :a,
        data,
        [:a, :test_proc],
        1,
        1,
        1,
        500,
        1_000,
        9999,
        500,
        700
      )
    end)

    send(:a, :crash)

    Enum.each(data, fn {key, _value} ->
      # try sending every message
      send(:a, %ClientRequest.Get{nonce: Nonce.new(), key: key})

      send(:a, %ClientRequest.Put{
        nonce: Nonce.new(),
        key: key,
        value: :new,
        context: %Context{version: %{a: 999}}
      })

      send(:a, %CoordinatorRequest.Get{
        nonce: Nonce.new(),
        key: key
      })

      send(:a, %CoordinatorRequest.Put{
        nonce: Nonce.new(),
        key: key,
        value: :new,
        context: %Context{version: %{a: 999}}
      })

      send(:a, %CoordinatorResponse.Get{
        nonce: Nonce.new(),
        values: [:new_1, :new_2],
        context: %Context{version: %{a: 999}}
      })

      send(:a, %CoordinatorResponse.Put{
        nonce: Nonce.new()
      })
    end)

    send(:a, :recover)

    refute_receive _msg, 1_000
  end

  test "Crashed node responds to messages after recovery" do
    spawn(:a, fn ->
      DynamoNode.start(:a, %{foo: 42}, [:a], 1, 1, 1, 500, 1_000, 9999, 500, 700)
    end)

    send(:a, :crash)
    send(:a, :recover)

    send(:a, %ClientRequest.Get{
      nonce: Nonce.new(),
      key: :foo
    })

    assert_receive _msg, 1_000
  end

  test "Node considers all nodes healthy on startup" do
    nodes = [:a, :b, :c]
    Cluster.start(%{}, nodes, 3, 1, 1, 1_000, 1_000, 200, 500, 700)

    expected_nodes_alive = Map.new(nodes, fn node -> {node, true} end)

    for node <- nodes do
      nonce = Nonce.new()
      send(node, %TestRequest{nonce: nonce})
      assert_receive %TestResponse{nonce: ^nonce, state: state}, 500
      assert state.nodes_alive == Map.delete(expected_nodes_alive, node)
    end
  end

  test "Coordinator considers crashed node dead after coord requests" do
    Cluster.start(
      %{foo: 42},
      [:a, :gonna_crash],
      2,
      2,
      2,
      1_000,
      1_000,
      200,
      500,
      700
    )

    send(:gonna_crash, :crash)

    # generate some traffic
    send(:a, %ClientRequest.Get{
      nonce: Nonce.new(),
      key: :foo
    })

    # wait for the dust to settle
    wait(1_200)

    nonce = Nonce.new()
    send(:a, %TestRequest{nonce: nonce})
    assert_receive %TestResponse{nonce: ^nonce, state: state}, 500
    assert state.nodes_alive == %{gonna_crash: false}
  end

  test "Coordinator considers recovered crashed node alive" do
    Cluster.start(
      %{foo: 42},
      [:a, :gonna_crash],
      2,
      2,
      2,
      1_000,
      1_000,
      200,
      500,
      700
    )

    send(:gonna_crash, :crash)

    # generate some traffic
    send(:a, %ClientRequest.Get{
      nonce: Nonce.new(),
      key: :foo
    })

    # wait for the dust to settle
    wait(1_200)

    send(:gonna_crash, :recover)

    wait(1000)

    nonce = Nonce.new()
    send(:a, %TestRequest{nonce: nonce})
    assert_receive %TestResponse{nonce: ^nonce, state: state}, 500
    assert state.nodes_alive == %{gonna_crash: true}
  end

  test "Follower considers crashed coordinator dead after trying to redirect" do
    data = Map.new(1..100, fn key -> {key, key * 42} end)
    Cluster.start(data, [:a, :gonna_crash], 1, 1, 1, 1_000, 1_000, 200, 500, 700)

    send(:gonna_crash, :crash)

    # generate bunch of traffic so that :gonna_crash becomes
    # a coordinator at least once
    Enum.each(data, fn {key, _val} ->
      send(:a, %ClientRequest.Get{
        nonce: Nonce.new(),
        key: key
      })
    end)

    # wait for the dust to settle
    wait(1_200)

    nonce = Nonce.new()
    send(:a, %TestRequest{nonce: nonce})
    assert_receive %TestResponse{nonce: ^nonce, state: state}, 500
    assert state.nodes_alive == %{gonna_crash: false}
  end

  test "Follower considers recovered crashed coordinator alive" do
    data = Map.new(1..100, fn key -> {key, key * 42} end)
    Cluster.start(data, [:a, :gonna_crash], 1, 1, 1, 1_000, 1_000, 200, 500, 700)

    send(:gonna_crash, :crash)

    # generate bunch of traffic so that :gonna_crash becomes
    # a coordinator at least once
    Enum.each(data, fn {key, _val} ->
      send(:a, %ClientRequest.Get{
        nonce: Nonce.new(),
        key: key
      })
    end)

    # wait for the dust to settle
    wait(1_200)

    send(:gonna_crash, :recover)

    wait(1000)

    nonce = Nonce.new()
    send(:a, %TestRequest{nonce: nonce})
    assert_receive %TestResponse{nonce: ^nonce, state: state}, 500
    assert state.nodes_alive == %{gonna_crash: true}
  end

  test "alive pref list is same as pref list if all alive" do
    nodes = Enum.to_list(1..50)
    ring = HashRing.new(nodes, 1)
    nodes_alive = Map.new(nodes, fn node -> {node, true} end)

    # mock up the useful bits of state
    for node <- nodes, key <- 1..200 do
      state = %DynamoNode{
        id: node,
        ring: ring,
        n: 20,
        nodes_alive: Map.delete(nodes_alive, node),
        # -- unused --
        store: nil,
        r: nil,
        w: nil,
        coordinator_timeout: nil,
        total_redirect_timeout: nil,
        request_timeout: nil,
        alive_check_interval: nil,
        replica_sync_timeout: nil,
        pending_gets: nil,
        pending_puts: nil,
        pending_redirects: nil,
        pending_handoffs: nil
      }

      assert DynamoNode.get_preference_list(state, key) ==
               DynamoNode.get_alive_preference_list(state, key)
    end
  end

  test "alive pref list doesn't contain dead nodes" do
    nodes = Enum.to_list(1..20)
    ring = HashRing.new(nodes, 1)

    nodes_alive =
      Map.new(nodes, fn node -> {node, Enum.random([true, false])} end)

    for {node, true} <- nodes_alive, key <- 1..100 do
      # mock up the useful bits of state
      state = %DynamoNode{
        id: node,
        ring: ring,
        n: 8,
        nodes_alive: Map.delete(nodes_alive, node),
        # -- unused --
        store: nil,
        r: nil,
        w: nil,
        coordinator_timeout: nil,
        total_redirect_timeout: nil,
        request_timeout: nil,
        alive_check_interval: nil,
        replica_sync_timeout: nil,
        pending_gets: nil,
        pending_puts: nil,
        pending_redirects: nil,
        pending_handoffs: nil
      }

      DynamoNode.get_alive_preference_list(state, key)
      |> Enum.each(fn node ->
        assert nodes_alive[node] == true
      end)
    end
  end

  test "alive pref list maintains order" do
    nodes = Enum.to_list(1..20)
    num_nodes = length(nodes)
    ring = HashRing.new(nodes, 1)

    nodes_alive =
      Map.new(nodes, fn node -> {node, Enum.random([true, false])} end)

    for {node, true} <- nodes_alive, key <- 1..100 do
      # mock up the useful bits of state
      state = %DynamoNode{
        id: node,
        ring: ring,
        n: 8,
        nodes_alive: Map.delete(nodes_alive, node),
        # -- unused --
        store: nil,
        r: nil,
        w: nil,
        coordinator_timeout: nil,
        total_redirect_timeout: nil,
        request_timeout: nil,
        alive_check_interval: nil,
        replica_sync_timeout: nil,
        pending_gets: nil,
        pending_puts: nil,
        pending_redirects: nil,
        pending_handoffs: nil
      }

      alive_pref_list = DynamoNode.get_alive_preference_list(state, key)

      whole_pref_list =
        DynamoNode.get_preference_list(%{state | n: num_nodes}, key)

      alive_pref_order =
        Enum.map(alive_pref_list, fn node ->
          Enum.find_index(whole_pref_list, &(&1 == node))
        end)

      assert nil not in alive_pref_order
      assert alive_pref_order == Enum.sort(alive_pref_order)
    end
  end

  test "coordinator sends correct hints for nodes it thinks are dead" do
    nodes = [:a, :b, :c, :d]

    Cluster.start(
      %{foo: 42},
      nodes,
      3,
      1,
      3,
      1000,
      1000,
      500,
      9999,
      700
    )

    ring = HashRing.new(nodes, 1)
    pref_order = HashRing.find_nodes(ring, :foo, 3)
    [first_pref, second_pref, _third_pref] = pref_order

    Logger.debug("preference order: #{inspect(pref_order)}")

    send(second_pref, :crash)

    # make first_pref know second_pref has crashed
    send(first_pref, %ClientRequest.Put{
      nonce: Nonce.new(),
      key: :foo,
      value: 100,
      context: new_context()
    })

    wait(1500)

    send(first_pref, %ClientRequest.Put{
      nonce: Nonce.new(),
      key: :foo,
      value: 500,
      context: %Context{version: %{first_pref => 1}}
    })

    wait(1000)

    # get states of all nodes (except the one that crashed)
    nonces =
      for node <- nodes, node != second_pref, into: [] do
        nonce = Nonce.new()
        send(node, %TestRequest{nonce: nonce})
        {node, nonce}
      end

    states =
      for {node, nonce} <- nonces, into: [] do
        assert_receive %TestResponse{
                         nonce: ^nonce,
                         state: state
                       },
                       500

        {node, state}
      end

    # check that atleast one of the states must contain a hint
    hinted_node_and_state =
      Enum.find(
        states,
        fn {_node, state} ->
          case Map.get(state.store, :foo) do
            nil -> false
            {_values, context} -> context.hint == second_pref
          end
        end
      )

    case hinted_node_and_state do
      nil ->
        raise "Hint to nobody"

      {node, state} ->
        assert node not in pref_order, "Hint to someone in top n"
        {values, _ctx} = Map.get(state.store, :foo)
        assert values == [500], "Wrong value"
    end
  end

  test "Put request succeeds even with a lot of crashed nodes in pref list" do
    nodes = [:a, :b, :c, :d, :e, :f]
    n = 4
    w = 4
    Cluster.start(%{foo: 42}, nodes, n, 1, w, 1000, 9999, 200, 9999, 9999)

    # figure out pref list
    pref_list = HashRing.find_nodes(HashRing.new(nodes, 1), :foo, 4)
    Logger.debug("preference list: #{inspect(pref_list)}")
    [pref_1, pref_2, pref_3, _pref_4] = pref_list

    # crash 2 nodes in pref list
    for node <- [pref_2, pref_3] do
      send(node, :crash)
    end

    nonce = Nonce.new()
    send(pref_1, %ClientRequest.Put{nonce: nonce, key: :foo, value: 49, context: new_context()})

    assert_receive %ClientResponse.Put{nonce: ^nonce, success: true, context: _context}, 1200
  end

  test "Coordinator figures out who's alive after put request" do
    nodes = [:a, :b, :c, :d, :e, :f]
    n = 4
    w = 4
    Cluster.start(%{foo: 42}, nodes, n, 1, w, 1000, 9999, 200, 9999, 9999)

    # figure out pref list
    pref_list = HashRing.find_nodes(HashRing.new(nodes, 1), :foo, 4)
    Logger.debug("preference list: #{inspect(pref_list)}")
    [pref_1, pref_2, pref_3, _pref_4] = pref_list

    # crash 2 nodes in pref list
    for node <- [pref_2, pref_3] do
      send(node, :crash)
    end

    nonce = Nonce.new()
    send(pref_1, %ClientRequest.Put{nonce: nonce, key: :foo, value: 49, context: new_context()})

    wait(1200)

    send(pref_1, %TestRequest{nonce: Nonce.new()})
    assert_receive %TestResponse{nonce: _nonce, state: pref_1_state}

    expected_nodes_alive =
      Map.new(nodes, fn node -> {node, true} end)
      |> Map.delete(pref_1)
      |> Map.put(pref_2, false)
      |> Map.put(pref_3, false)

    assert pref_1_state.nodes_alive == expected_nodes_alive
  end

  test "Coordinator figures out who's alive after get request" do
    nodes = [:a, :b, :c, :d, :e, :f]
    n = 4
    r = 4
    Cluster.start(%{foo: 42}, nodes, n, r, 1, 1000, 9999, 200, 9999, 9999)

    # figure out pref list
    pref_list = HashRing.find_nodes(HashRing.new(nodes, 1), :foo, Enum.count(nodes))
    Logger.debug("preference list: #{inspect(pref_list)}")
    [pref_1, pref_2, pref_3, _pref_4, _pref_5, pref_6] = pref_list

    # crash 2 nodes in pref list
    crash_nodes = [pref_2, pref_3, pref_6]
    for node <- crash_nodes do
      send(node, :crash)
    end

    nonce = Nonce.new()
    send(pref_1, %ClientRequest.Get{nonce: nonce, key: :foo})

    wait(1200)

    send(pref_1, %TestRequest{nonce: Nonce.new()})
    assert_receive %TestResponse{nonce: _nonce, state: pref_1_state}

    expected_nodes_alive =
      Map.new(nodes, fn node -> {node, node not in crash_nodes} end)
      |> Map.delete(pref_1)

    assert pref_1_state.nodes_alive == expected_nodes_alive
  end
end
