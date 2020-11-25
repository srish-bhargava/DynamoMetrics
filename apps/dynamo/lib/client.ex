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
    nonce = Nonce.new()

    send(node, %ClientRequest.Get{
      nonce: nonce,
      key: key
    })

    receive do
      {_node,
       %ClientResponse.Get{
         nonce: ^nonce
       } = msg} ->
        msg
    end
  end

  @doc """
  Contact a node to insert/replace the value of a key.
  """
  def put(node, key, context, value) do
    nonce = Nonce.new()

    send(node, %ClientRequest.Put{
      nonce: nonce,
      key: key,
      value: value,
      context: context
    })

    receive do
      {_node,
       %ClientResponse.Put{
         nonce: ^nonce
       } = msg} ->
        msg
    end
  end
end
