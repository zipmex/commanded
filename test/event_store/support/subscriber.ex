defmodule Commanded.EventStore.Subscriber do
  use GenServer

  alias Commanded.EventStore
  alias Commanded.EventStore.Subscriber

  defmodule State do
    defstruct [
      :recipient,
      :subscription,
      opts: [],
      received_events: [],
      subscribed?: false
    ]
  end

  alias Subscriber.State

  def start_link(recipient, opts \\ []) when is_pid(recipient) do
    GenServer.start_link(__MODULE__, %State{recipient: recipient, opts: opts})
  end

  def start(recipient, opts \\ []) when is_pid(recipient) do
    GenServer.start(__MODULE__, %State{recipient: recipient, opts: opts})
  end

  def init(%State{} = state) do
    %State{opts: opts} = state

    case EventStore.subscribe_to(:all, "subscriber", self(), opts) do
      {:ok, subscription} ->
        state = %State{state | subscription: subscription}

        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def ack(subscriber, events), do: GenServer.call(subscriber, {:ack, events})

  def subscribed?(subscriber), do: GenServer.call(subscriber, :subscribed?)

  def received_events(subscriber), do: GenServer.call(subscriber, :received_events)

  def handle_call({:ack, events}, _from, %State{} = state) do
    %State{subscription: subscription} = state

    :ok = EventStore.ack_event(subscription, List.last(events))

    {:reply, :ok, state}
  end

  def handle_call(:subscribed?, _from, %State{} = state) do
    %State{subscribed?: subscribed?} = state

    {:reply, subscribed?, state}
  end

  def handle_call(:received_events, _from, %State{} = state) do
    %State{received_events: received_events} = state

    {:reply, received_events, state}
  end

  def handle_info({:subscribed, subscription}, %State{subscription: subscription} = state) do
    %State{recipient: recipient} = state

    send(recipient, {:subscribed, subscription})

    {:noreply, %State{state | subscribed?: true}}
  end

  def handle_info({:events, events}, %State{} = state) do
    %State{recipient: recipient, received_events: received_events} = state

    send(recipient, {:events, self(), events})

    state = %State{state | received_events: received_events ++ events}

    {:noreply, state}
  end
end
