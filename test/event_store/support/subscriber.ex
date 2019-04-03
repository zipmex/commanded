defmodule Commanded.EventStore.Subscriber do
  use GenServer

  alias Commanded.EventStore
  alias Commanded.EventStore.Subscriber

  defmodule State do
    defstruct [
      :owner,
      :concurrency,
      :subscribe_to,
      :subscription,
      :start_from,
      received_events: [],
      subscribed?: false
    ]
  end

  alias Subscriber.State

  def start_link(owner, opts \\ []) when is_pid(owner) do
    GenServer.start_link(__MODULE__, state(owner, opts))
  end

  def start(owner, opts \\ []) when is_pid(owner) do
    GenServer.start(__MODULE__, state(owner, opts))
  end

  def init(%State{} = state) do
    %State{concurrency: concurrency, start_from: start_from, subscribe_to: subscribe_to} = state

    opts = [start_from: start_from, concurrency: concurrency]

    case EventStore.subscribe_to(subscribe_to, "subscriber", self(), opts) do
      {:ok, subscription} ->
        state = %State{state | subscription: subscription}

        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def subscribed?(subscriber), do: GenServer.call(subscriber, :subscribed?)

  def received_events(subscriber), do: GenServer.call(subscriber, :received_events)

  def handle_call(:subscribed?, _from, %State{} = state) do
    %State{subscribed?: subscribed?} = state

    {:reply, subscribed?, state}
  end

  def handle_call(:received_events, _from, %State{} = state) do
    %State{received_events: received_events} = state

    {:reply, received_events, state}
  end

  def handle_info({:subscribed, subscription}, %State{subscription: subscription} = state) do
    %State{owner: owner} = state

    send(owner, {:subscribed, subscription})

    {:noreply, %State{state | subscribed?: true}}
  end

  def handle_info({:events, events}, %State{} = state) do
    %State{owner: owner, received_events: received_events, subscription: subscription} = state

    send(owner, {:events, subscription, events})

    state = %State{state | received_events: received_events ++ events}

    {:noreply, state}
  end

  defp state(owner, opts) do
    %State{
      owner: owner,
      concurrency: Keyword.get(opts, :concurrency, 1),
      start_from: Keyword.get(opts, :start_from, :origin),
      subscribe_to: Keyword.get(opts, :subscribe_to, :all)
    }
  end
end
