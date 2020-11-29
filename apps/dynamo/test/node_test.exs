defmodule DynamoNodeTest do
  use ExUnit.Case
  doctest DynamoNode

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

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

  describe "No crashes" do
    test "during startup of a single node" do
      handle =
        Process.monitor(
          spawn(:node, fn ->
            DynamoNode.start(:node, %{}, [:node], 1, 1, 1, 1_000, 9999)
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
            DynamoNode.start(node, %{}, nodes, 1, 1, 1, 1_000, 9999)
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
            DynamoNode.start(node, %{foo: 42}, nodes, 1, 1, 1, 1_000, 9999)
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
            DynamoNode.start(node, %{foo: 42}, nodes, 1, 1, 1, 1_000, 9999)
          end)
        )
      end

      send(:a, %ClientRequest.Put{
        nonce: Nonce.new(),
        key: :foo,
        value: 49,
        context: %Context{version: VectorClock.new()}
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
    nonce = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{foo: 42}, [:node], 1, 1, 1, 1_000, 9999)
    end)

    send(:node, %ClientRequest.Get{nonce: nonce, key: :foo})

    assert_receive %ClientResponse.Get{
                     nonce: ^nonce,
                     success: true,
                     values: [42],
                     context: _context
                   },
                   5_000
  end

  test "Simple put request is successful" do
    nonce = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1, 1_000, 9999)
    end)

    send(:node, %ClientRequest.Put{
      nonce: nonce,
      key: :foo,
      value: 42,
      context: %Context{version: VectorClock.new()}
    })

    assert_receive %ClientResponse.Put{nonce: ^nonce, success: true},
                   5_000
  end

  test "get after a put returns the put value with empty initial data" do
    nonce_put = Nonce.new()
    nonce_get = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1, 1_000, 9999)
    end)

    send(:node, %ClientRequest.Put{
      nonce: nonce_put,
      key: :foo,
      value: 42,
      context: %Context{version: VectorClock.new()}
    })

    send(:node, %ClientRequest.Get{nonce: nonce_get, key: :foo})

    assert_receive %ClientResponse.Get{
                     nonce: ^nonce_get,
                     success: true,
                     values: [42],
                     context: _context
                   },
                   5_000
  end

  test "put request overwrites key in initial data" do
    nonce_put = Nonce.new()
    nonce_get = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{foo: 37}, [:node], 1, 1, 1, 1_000, 9999)
    end)

    send(:node, %ClientRequest.Put{
      nonce: nonce_put,
      key: :foo,
      value: 42,
      context: %Context{version: VectorClock.new()}
    })

    send(:node, %ClientRequest.Get{nonce: nonce_get, key: :foo})

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

    Enum.each(nodes, fn node ->
      spawn(node, fn ->
        DynamoNode.start(node, data, nodes, 4, 3, 2, 1_000, 9999)
      end)
    end)

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

    Enum.each(nodes, fn node ->
      spawn(node, fn ->
        DynamoNode.start(node, data, nodes, 4, 3, 2, 1_000, 9999)
      end)
    end)

    for {key, _value} <- data do
      nonce = Nonce.new()

      send(:c, %ClientRequest.Put{
        nonce: nonce,
        key: key,
        value: 100,
        context: %Context{version: VectorClock.new()}
      })
    end

    # wait a bit for the system to settle
    receive do
    after
      1_000 -> true
    end

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
      DynamoNode.start(:a, %{foo: 42}, [:a, :b, :c], 3, 3, 3, 500, 9999)
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
      DynamoNode.start(:a, %{foo: 42}, [:a, :b, :c], 3, 3, 3, 500, 9999)
    end)

    nonce = Nonce.new()

    send(:a, %ClientRequest.Put{
      nonce: nonce,
      key: :foo,
      value: 49,
      context: %Context{version: VectorClock.new()}
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
      DynamoNode.start(:a, data, [:a, :test_proc], 1, 1, 1, 500, 9999)
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
      DynamoNode.start(:a, data, [:a, :test_proc], 1, 1, 1, 500, 9999)
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

    refute_receive msg,
                   1_000,
                   "Received response (#{inspect(msg)}) for some request"
  end

  test "Crashed node responds to messages after recovery" do
    spawn(:a, fn ->
      DynamoNode.start(:a, %{foo: 42}, [:a], 1, 1, 1, 500, 9999)
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
    Cluster.start(%{}, nodes, 3, 1, 1, 1_000, 200)

    expected_nodes_alive = Map.new(nodes, fn node -> {node, true} end)

    for node <- nodes do
      nonce = Nonce.new()
      send(node, %TestRequest{nonce: nonce})
      assert_receive %TestResponse{nonce: ^nonce, state: state}, 500
      assert state.nodes_alive == Map.delete(expected_nodes_alive, node)
    end
  end

  test "All nodes consider crashed node dead after lots of traffic" do
    nodes = [:a, :b, :c]
    data = Map.new(1..1000, fn key -> {key, key * 42} end)

    Cluster.start(data, [:gonna_crash | nodes], 3, 3, 3, 1_000, 200)

    send(:gonna_crash, :crash)

    # generate lots of traffic
    Enum.each(data, fn {key, _val} ->
      send(:a, %ClientRequest.Get{
        nonce: Nonce.new(),
        key: key
      })
    end)

    # wait for the dust to settle
    receive do
    after
      2_000 -> true
    end

    expected_nodes_alive =
      Map.new(nodes, fn node -> {node, true} end)
      |> Map.put(:gonna_crash, false)

    for node <- nodes do
      nonce = Nonce.new()
      send(node, %TestRequest{nonce: nonce})
      assert_receive %TestResponse{nonce: ^nonce, state: state}, 500
      assert state.nodes_alive == Map.delete(expected_nodes_alive, node)
    end
  end
end
