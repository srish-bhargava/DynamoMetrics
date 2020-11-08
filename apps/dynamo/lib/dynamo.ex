defmodule Dynamo do
  @moduledoc """
  Simplified implementation of Amazon's Dynamo DB.
  """
  # override Kernel's functions with Emulation's
  import Emulation, only: [spawn: 2, send: 2, timer: 1, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  def start(data, nodes, N, R, W) do
    Enum.each(nodes, fn node ->
      spawn(node, fn ->
        DynamoNode.start(id: node, store: data, nodes: nodes, N: N, R: R, W: W)
      end)
    end)
  end
end
