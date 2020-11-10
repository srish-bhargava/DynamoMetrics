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

      send(:a, %ClientRequest.Get{key: :foo})

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

      send(:a, %ClientRequest.Put{key: :foo, value: 49})

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
    spawn(:node, fn ->
      DynamoNode.start(:node, %{foo: 42}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Get{key: :foo})

    assert_receive {_node,
                    %ClientResponse.Get{
                      success: true,
                      key: :foo,
                      values: [{42, _clock}]
                    }},
                   5_000
  end

  test "Simple put request is successful" do
    spawn(:node, fn ->
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Put{key: :foo, value: 42})

    assert_receive {_node, %ClientResponse.Put{success: true, key: :foo}},
                   5_000
  end

  test "get after a put returns the put value with empty initial data" do
    spawn(:node, fn ->
      DynamoNode.start(:node, %{}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Put{key: :foo, value: 42})
    send(:node, %ClientRequest.Get{key: :foo})

    assert_receive {_node,
                    %ClientResponse.Get{success: true, values: [{42, _clock}]}},
                   5_000
  end

  test "put request overwrites key in initial data" do
    spawn(:node, fn ->
      DynamoNode.start(:node, %{foo: 37}, [:node], 1, 1, 1)
    end)

    send(:node, %ClientRequest.Put{key: :foo, value: 42})
    send(:node, %ClientRequest.Get{key: :foo})

    assert_receive {_node,
                    %ClientResponse.Get{success: true, values: [{42, _clock}]}},
                   5_000
  end
end
