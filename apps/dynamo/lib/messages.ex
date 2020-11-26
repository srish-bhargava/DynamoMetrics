defmodule Context do
  use TypedStruct

  typedstruct enforce: true do
    @typedoc """
    Context for values in a key-value store.
    """
    field :version, VectorClock.t()
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
    field :values, [any()]
    field :context, %Context{}
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
    field :context, %Context{}
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
