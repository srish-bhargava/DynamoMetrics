defmodule ClientRequest.Get do
  @moduledoc """
  Message from a client for a `get` request.
  """
  defstruct(key: nil)
end

defmodule ClientResponse.Get do
  @moduledoc """
  Message from a dynamo node to a client in response
  to a `get` request.
  """
  defstruct(
    success: nil,
    key: nil,
    # collection of values and versions
    values: nil
  )
end

defmodule ClientRequest.Put do
  @moduledoc """
  Message from a client for a `put` request.
  """
  defstruct(
    key: nil,
    value: nil
  )
end

defmodule ClientResponse.Put do
  @moduledoc """
  Message from a dynamo node to a client in response
  to a `put` request.
  """
  defstruct(
    success: nil,
    key: nil
  )
end

defmodule CoordinatorRequest.Get do
  @moduledoc """
  Message from a coordinator to a dynamo node to `get` a key.
  """
  defstruct(key: nil)
end

defmodule CoordinatorResponse.Get do
  @moduledoc """
  Message from a dynamo node to a coordinator in response to a `get`.
  """
  defstruct(
    key: nil,
    values: nil
  )
end

defmodule CoordinatorRequest.Put do
  @moduledoc """
  Message from a coordinator to a dynamo node to `put` a key=value.
  """
  defstruct(
    key: nil,
    value: nil,
    clock: nil
  )
end

defmodule CoordinatorResponse.Put do
  @moduledoc """
  Message from a dynamo node to a coordinator in response to a `put`.
  """
  defstruct(key: nil)
end

defmodule RedirectedClientRequest do
  @moduledoc """
  A redirected client request.
  """
  defstruct(
    client: nil,
    request: nil
  )
end
