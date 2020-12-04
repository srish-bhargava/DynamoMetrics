defmodule Context do
  use TypedStruct

  typedstruct do
    @typedoc """
    Context for values in a key-value store.
    """
    field :version, VectorClock.t(), enforce: true
    field :hint, any() | nil, default: nil
  end

  def compare(ctx1, ctx2) do
    VectorClock.compare(ctx1, ctx2)
  end

  def combine(ctx1, ctx2) do
    case compare(ctx1, ctx2) do
      :before ->
        ctx2

      :after ->
        ctx1

      :concurrent ->
        %Context{
          version: VectorClock.combine(ctx1.version, ctx2.version),
          hint:
            if ctx1.hint != nil do
              ctx1.hint
            else
              ctx2.hint
            end
        }
    end
  end
end

defmodule ClientRequest.Get do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a client for a `get` request.
    """
    field :nonce, Nonce.t()
    field :key, any()
  end
end

defmodule ClientResponse.Get do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a dynamo node to a client in response
    to a `get` request.
    """
    field :nonce, Nonce.t()
    field :success, boolean()
    # one or more concurrent values held by the system
    field :values, [any()] | nil
    field :context, %Context{} | nil
  end
end

defmodule ClientRequest.Put do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a client for a `put` request.
    """
    field :nonce, Nonce.t()
    field :key, any()
    field :value, any()
    field :context, %Context{}
  end
end

defmodule ClientResponse.Put do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a dynamo node to a client in response
    to a `put` request.
    """
    field :nonce, Nonce.t()
    field :success, boolean()
    field :context, %Context{} | nil
  end
end

defmodule CoordinatorRequest.Get do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a coordinator to a dynamo node to `get` a key.
    """
    field :nonce, Nonce.t()
    field :key, any()
  end
end

defmodule CoordinatorResponse.Get do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a dynamo node to a coordinator in response to a `get`.
    """
    field :nonce, Nonce.t()
    field :values, [any()]
    field :context, %Context{}
  end
end

defmodule CoordinatorRequest.Put do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a coordinator to a dynamo node to `put` a key=value.
    """
    field :nonce, Nonce.t()
    field :key, any()
    field :value, any()
    field :context, %Context{}
  end
end

defmodule CoordinatorResponse.Put do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message from a dynamo node to a coordinator in response to a `put`.
    """
    field :nonce, Nonce.t()
  end
end

defmodule RedirectedClientRequest do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    A redirected client request.
    """
    field :client, any()
    field :request, %ClientRequest.Get{} | %ClientRequest.Put{}
  end
end

defmodule HandoffRequest do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Handoff hinted data to original owner.
    """
    field :nonce, Nonce.t()
    field :data, %{required(any()) => {[any()], %Context{}}}
  end
end

defmodule HandoffResponse do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Handoff acknowledgement.
    """
    field :nonce, Nonce.t()
  end
end

defmodule TestRequest do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message for retrieving information for tests.
    """
    field :nonce, Nonce.t()
  end
end

defmodule TestResponse do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Message for retrieving information for tests.
    """
    field :nonce, Nonce.t()
    field :state, %DynamoNode{}
  end
end
