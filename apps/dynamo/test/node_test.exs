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
    :ok
  end

  describe "No crashes" do
    test "during startup of a single node" do
      handle =
        Process.monitor(
          spawn(:node, fn ->
            DynamoNode.start(:node, %{}, [:node], 1, 1, 1, 1_000)
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
            DynamoNode.start(node, %{}, nodes, 1, 1, 1, 1_000)
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
            DynamoNode.start(node, %{foo: 42}, nodes, 1, 1, 1, 1_000)
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
            DynamoNode.start(node, %{foo: 42}, nodes, 1, 1, 1, 1_000)
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
      DynamoNode.start(:node, %{foo: 42}, [:node], 1, 1, 1, 1_000)
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
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1, 1_000)
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
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1, 1_000)
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
      DynamoNode.start(:node, %{foo: 37}, [:node], 1, 1, 1, 1_000)
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
        DynamoNode.start(node, data, nodes, 4, 3, 2, 1_000)
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
        DynamoNode.start(node, data, nodes, 4, 3, 2, 1_000)
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
      DynamoNode.start(:a, %{foo: 42}, [:a, :b, :c], 3, 3, 3, 500)
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
      DynamoNode.start(:a, %{foo: 42}, [:a, :b, :c], 3, 3, 3, 500)
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
end
