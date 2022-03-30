defprotocol Commanded.Event.EventIdentity do
  @type uuid :: String.t()

  @fallback_to_any true
  @spec event_id(event :: struct(), metadata :: map()) :: uuid() | nil
  def event_id(event, metadata)
end

defimpl Commanded.Event.EventIdentity, for: Any do
  def event_id(_event, _metadata), do: nil
end
