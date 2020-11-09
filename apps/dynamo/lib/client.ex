defmodule Client do
  @moduledoc """
  Client for a DynamoDB cluster.

  The client can make get and put requests, and not much else.
  Importantly, the client is NOT aware of the node partitions,
  and relies on redirections to reach the coordinator.
  """

  @doc """
  Contact a node to get the value of a key.
  """
  def get(node, key) do
    send(node, %ClientRequest.Get{
      key: key
    })

    receive do
      {_node,
       %ClientResponse.Get{
         key: ^key
       } = msg} ->
        msg
    end
  end

  @doc """
  Contact a node to insert/replace the value of a key.
  """
  def put(node, key, value) do
    send(node, %ClientRequest.Put{
      key: key,
      value: value
    })

    receive do
      {_node,
       %ClientResponse.Put{
         key: ^key
       } = msg} ->
        msg
    end
  end
end
