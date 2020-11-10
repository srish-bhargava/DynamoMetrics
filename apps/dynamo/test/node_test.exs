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
            DynamoNode.start(:node, %{}, [:node], 1, 1, 1)
          end)
        )

      receive do
        {:DOWN, ^handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        5_000 ->
          true
      end
    end

    test "during startup of multiple nodes" do
      nodes = [:a, :b, :c]

      for node <- nodes do
        Process.monitor(
          spawn(node, fn ->
            DynamoNode.start(node, %{}, nodes, 1, 1, 1)
          end)
        )
      end

      receive do
        {:DOWN, _handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        5_000 ->
          true
      end
    end

    test "on a get request" do
      nodes = [:a, :b, :c]

      for node <- nodes do
        Process.monitor(
          spawn(node, fn ->
            DynamoNode.start(node, %{foo: 42}, nodes, 1, 1, 1)
          end)
        )
      end

      send(:a, %ClientRequest.Get{nonce: Nonce.new(), key: :foo})

      receive do
        {:DOWN, _handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        5_000 ->
          true
      end
    end

    test "on a put request" do
      nodes = [:a, :b, :c]

      for node <- nodes do
        Process.monitor(
          spawn(node, fn ->
            DynamoNode.start(node, %{foo: 42}, nodes, 1, 1, 1)
          end)
        )
      end

      send(:a, %ClientRequest.Put{nonce: Nonce.new(), key: :foo, value: 49})

      receive do
        {:DOWN, _handle, _, proc, reason} ->
          assert false, "node #{inspect(proc)} crashed (reason: #{reason})"
      after
        5_000 ->
          true
      end
    end
  end

  test "First get request returns the initial value" do
    nonce = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{foo: 42}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Get{nonce: nonce, key: :foo})

    assert_receive {_node,
                    %ClientResponse.Get{
                      nonce: ^nonce,
                      success: true,
                      values: [{42, _clock}]
                    }},
                   5_000
  end

  test "Simple put request is successful" do
    nonce = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Put{nonce: nonce, key: :foo, value: 42})

    assert_receive {_node, %ClientResponse.Put{nonce: ^nonce, success: true}},
                   5_000
  end

  test "get after a put returns the put value with empty initial data" do
    nonce_put = Nonce.new()
    nonce_get = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Put{nonce: nonce_put, key: :foo, value: 42})
    send(:node, %ClientRequest.Get{nonce: nonce_get, key: :foo})

    assert_receive {_node,
                    %ClientResponse.Get{
                      nonce: ^nonce_get,
                      success: true,
                      values: [{42, _clock}]
                    }},
                   5_000
  end

  test "put request overwrites key in initial data" do
    nonce_put = Nonce.new()
    nonce_get = Nonce.new()

    spawn(:node, fn ->
      DynamoNode.start(:node, %{foo: 37}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Put{nonce: nonce_put, key: :foo, value: 42})
    send(:node, %ClientRequest.Get{nonce: nonce_get, key: :foo})

    assert_receive {_node,
                    %ClientResponse.Get{
                      nonce: ^nonce_get,
                      success: true,
                      values: [{42, _clock}]
                    }},
                   5_000
  end
end
