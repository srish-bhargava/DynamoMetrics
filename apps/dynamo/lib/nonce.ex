defmodule Nonce do
  @moduledoc """
  Generate pseudo-random nonces for messages.
  """

  @max_nonce 1_000_000_000

  @doc """
  Generate a new nonce, making sure to seed
  with the current time if not already seeded.
  """
  def new do
    if :rand.export_seed() == :undefined do
      :rand.seed(:exrop, :erlang.now())
    end

    :rand.uniform(@max_nonce)
  end
end
