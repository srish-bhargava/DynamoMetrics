defmodule Cluster do
  @moduledoc """
  A DynamoDB cluster.
  """
  # override Kernel's functions with Emulation's
  import Emulation, only: [spawn: 2, send: 2, timer: 1, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  @doc """
  Start a dynamo cluster.
  """
  @spec start(map(), [any()], pos_integer(), pos_integer(), pos_integer()) ::
          :ok
  def start(data, nodes, n, r, w) do
    Enum.each(nodes, fn node ->
      spawn(node, fn ->
        DynamoNode.start(node, data, nodes, n, r, w)
      end)
    end)
  end
end
