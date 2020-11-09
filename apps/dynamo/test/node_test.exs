defmodule DynamoNodeTest do
  use ExUnit.Case
  doctest Node

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "First get request returns the initial value" do
    Emulation.init()

    node =
      spawn(:node, fn ->
        DynamoNode.start(:node, %{foo: 42}, [:node], 1, 1, 1)
      end)

    send(:node, %ClientRequest.Get{key: :foo})

    node_handle = Process.monitor(node)

    receive do
      {_node, %ClientResponse.Get{success: success, key: :foo, values: values}} ->
        assert success == true, "get request unsuccessful even with perfect conditions"
        assert [{42, _clock}] = values, "wrong or multiple values received for key"

      {:DOWN, ^node_handle, _, proc, reason} ->
        assert false, "node #{inspect(proc)} crashed (reason: #{reason})"

      msg ->
        assert false, "unexpected msg received: #{inspect(msg)}"
    after
      5_000 ->
        assert false, "no message received"
    end
  after
    Emulation.terminate()
  end

  test "Simple put request is successful" do
    Emulation.init()

    node =
      spawn(:node, fn ->
        DynamoNode.start(:node, %{}, [:node], 1, 1, 1)
      end)

    node_handle = Process.monitor(node)

    send(:node, %ClientRequest.Put{key: :foo, value: 42})

    receive do
      {_node, %ClientResponse.Put{success: success, key: :foo}} ->
        assert success == true, "put request unsuccessful even with perfect conditions"

      {:DOWN, ^node_handle, _, proc, reason} ->
        assert false, "node #{inspect(proc)} crashed (reason: #{reason})"

      msg ->
        assert false, "unexpected msg received: #{inspect(msg)}"
    after
      5_000 ->
        assert false, "no message received"
    end
  after
    Emulation.terminate()
  end

  test "get after a put returns the put value with empty initial data" do
    Emulation.init()

    node =
      spawn(:node, fn ->
        DynamoNode.start(:node, %{}, [:node], 1, 1, 1)
      end)

    node_handle = Process.monitor(node)

    send(:node, %ClientRequest.Put{key: :foo, value: 42})

    receive do
      _ -> true
    end

    send(:node, %ClientRequest.Get{key: :foo})

    receive do
      {_node, %ClientResponse.Get{success: success, key: :foo, values: values}} ->
        assert success == true
        assert [{42, _clock}] = values, "different value(s) returned from :get"
    after
      5_000 ->
        assert false, "no message received"
    end
  after
    Emulation.terminate()
  end

  test "put request overwrites key in initial data" do
    Emulation.init()

    node =
      spawn(:node, fn ->
        DynamoNode.start(:node, %{foo: 37}, [:node], 1, 1, 1)
      end)

    node_handle = Process.monitor(node)

    send(:node, %ClientRequest.Put{key: :foo, value: 42})

    receive do
      _ -> true
    end

    send(:node, %ClientRequest.Get{key: :foo})

    receive do
      {_node, %ClientResponse.Get{success: success, key: :foo, values: values}} ->
        refute [{37, _clock}] = values, "put did not overwrite key's value"
        assert [{42, _clock}] = values, "different value returned from :get"
    after
      5_000 ->
        assert false, "no message received"
    end
  after
    Emulation.terminate()
  end
end
