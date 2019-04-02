defmodule Commanded.EventStore.Adapters.InMemory do
  @moduledoc """
  An in-memory event store adapter useful for testing as no persistence provided.
  """

  @behaviour Commanded.EventStore

  use GenServer

  defmodule State do
    @moduledoc false
    defstruct [
      :serializer,
      persisted_events: [],
      streams: %{},
      transient_subscribers: %{},
      persistent_subscriptions: %{},
      snapshots: %{},
      next_event_number: 1
    ]
  end

  defmodule PersistentSubscription do
    @moduledoc false
    defstruct [
      :stream_uuid,
      :name,
      :ref,
      :start_from,
      :concurrency,
      subscriptions: [],
      last_seen: 0
    ]
  end

  alias Commanded.EventStore.Adapters.InMemory.State
  alias Commanded.EventStore.Adapters.InMemory.Subscription
  alias Commanded.EventStore.Adapters.InMemory.PersistentSubscription
  alias Commanded.EventStore.EventData
  alias Commanded.EventStore.RecordedEvent
  alias Commanded.EventStore.SnapshotData

  def start_link(opts \\ []) do
    state = %State{serializer: Keyword.get(opts, :serializer)}

    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  @impl GenServer
  def init(%State{} = state) do
    {:ok, state}
  end

  @impl Commanded.EventStore
  def child_spec do
    opts = Application.get_env(:commanded, __MODULE__)

    [
      child_spec(opts),
      {DynamicSupervisor, strategy: :one_for_one, name: __MODULE__.SubscriptionsSupervisor}
    ]
  end

  @impl Commanded.EventStore
  def append_to_stream(stream_uuid, expected_version, events) do
    GenServer.call(__MODULE__, {:append, stream_uuid, expected_version, events})
  end

  @impl Commanded.EventStore
  def stream_forward(stream_uuid, start_version \\ 0, _read_batch_size \\ 1_000) do
    GenServer.call(__MODULE__, {:stream_forward, stream_uuid, start_version})
  end

  @impl Commanded.EventStore
  def subscribe(stream_uuid) do
    GenServer.call(__MODULE__, {:subscribe, stream_uuid, self()})
  end

  @impl Commanded.EventStore
  def subscribe_to(stream_uuid, subscription_name, subscriber, opts) do
    subscription = %PersistentSubscription{
      stream_uuid: stream_uuid,
      name: subscription_name,
      start_from: Keyword.get(opts, :start_from, :origin),
      concurrency: Keyword.get(opts, :concurrency, 1)
    }

    GenServer.call(__MODULE__, {:subscribe_to, subscription, subscriber})
  end

  @impl Commanded.EventStore
  def ack_event(pid, event) do
    GenServer.cast(__MODULE__, {:ack_event, event, pid})
  end

  @impl Commanded.EventStore
  def unsubscribe(subscription) do
    GenServer.call(__MODULE__, {:unsubscribe, subscription})
  end

  @impl Commanded.EventStore
  def delete_subscription(stream_uuid, subscription_name) do
    GenServer.call(__MODULE__, {:delete_subscription, stream_uuid, subscription_name})
  end

  @impl Commanded.EventStore
  def read_snapshot(source_uuid) do
    GenServer.call(__MODULE__, {:read_snapshot, source_uuid})
  end

  @impl Commanded.EventStore
  def record_snapshot(snapshot) do
    GenServer.call(__MODULE__, {:record_snapshot, snapshot})
  end

  @impl Commanded.EventStore
  def delete_snapshot(source_uuid) do
    GenServer.call(__MODULE__, {:delete_snapshot, source_uuid})
  end

  def reset! do
    GenServer.call(__MODULE__, :reset!)
  end

  @impl GenServer
  def handle_call({:append, stream_uuid, :stream_exists, events}, _from, %State{} = state) do
    %State{streams: streams} = state

    {reply, state} =
      case Map.get(streams, stream_uuid) do
        nil ->
          {{:error, :stream_does_not_exist}, state}

        existing_events ->
          persist_events(stream_uuid, existing_events, events, state)
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:append, stream_uuid, :no_stream, events}, _from, %State{} = state) do
    %State{streams: streams} = state

    {reply, state} =
      case Map.get(streams, stream_uuid) do
        nil ->
          persist_events(stream_uuid, [], events, state)

        _existing_events ->
          {{:error, :stream_exists}, state}
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:append, stream_uuid, :any_version, events}, _from, %State{} = state) do
    %State{streams: streams} = state

    existing_events = Map.get(streams, stream_uuid, [])

    {:ok, state} = persist_events(stream_uuid, existing_events, events, state)

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:append, stream_uuid, expected_version, events}, _from, %State{} = state)
      when is_integer(expected_version) do
    %State{streams: streams} = state

    {reply, state} =
      case Map.get(streams, stream_uuid) do
        nil ->
          case expected_version do
            0 ->
              persist_events(stream_uuid, [], events, state)

            _ ->
              {{:error, :wrong_expected_version}, state}
          end

        existing_events
        when length(existing_events) != expected_version ->
          {{:error, :wrong_expected_version}, state}

        existing_events ->
          persist_events(stream_uuid, existing_events, events, state)
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:stream_forward, stream_uuid, start_version}, _from, %State{} = state) do
    %State{streams: streams} = state

    reply =
      case Map.get(streams, stream_uuid) do
        nil ->
          {:error, :stream_not_found}

        events ->
          events
          |> Enum.reverse()
          |> Stream.drop(max(0, start_version - 1))
          |> Stream.map(&deserialize(&1, state))
          |> set_event_number_from_version()
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:subscribe, stream_uuid, subscriber}, _from, %State{} = state) do
    %State{transient_subscribers: transient_subscribers} = state

    Process.monitor(subscriber)

    transient_subscribers =
      Map.update(transient_subscribers, stream_uuid, [subscriber], fn transient ->
        [subscriber | transient]
      end)

    {:reply, :ok, %State{state | transient_subscribers: transient_subscribers}}
  end

  @impl GenServer
  def handle_call(
        {:subscribe_to, %PersistentSubscription{} = subscription, subscriber},
        _from,
        %State{} = state
      ) do
    %PersistentSubscription{name: subscription_name} = subscription
    %State{persistent_subscriptions: subscriptions} = state

    {reply, state} =
      case Map.get(subscriptions, subscription_name) do
        nil ->
          persistent_subscription(subscription, subscriber, state)

        %PersistentSubscription{concurrency: 1} ->
          {{:error, :subscription_already_exists}, state}

        %PersistentSubscription{concurrency: concurrency} = subscription ->
          %PersistentSubscription{subscriptions: subscriptions} = subscription

          if length(subscriptions) < concurrency do
            persistent_subscription(subscription, subscriber, state)
          else
            {{:error, :too_many_subscribers}, state}
          end
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:unsubscribe, pid}, _from, %State{} = state) do
    %State{persistent_subscriptions: persistent_subscriptions} = state

    {reply, state} =
      case persistent_subscription_by_pid(persistent_subscriptions, pid) do
        {name, %PersistentSubscription{} = subscription} ->
          %PersistentSubscription{subscriptions: subscriptions} = subscription

          :ok = stop_subscription(pid)

          subscription = %PersistentSubscription{
            subscription
            | subscriptions: subscriptions -- [pid]
          }

          state = %State{
            state
            | persistent_subscriptions: Map.put(persistent_subscriptions, name, subscription)
          }

          {:ok, state}

        nil ->
          {:ok, state}
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:delete_subscription, stream_uuid, subscription_name}, _from, %State{} = state) do
    %State{persistent_subscriptions: persistent_subscriptions} = state

    {reply, state} =
      case Map.get(persistent_subscriptions, subscription_name) do
        %PersistentSubscription{stream_uuid: ^stream_uuid, subscriptions: []} ->
          state = %State{
            state
            | persistent_subscriptions: Map.delete(persistent_subscriptions, subscription_name)
          }

          {:ok, state}

        nil ->
          {{:error, :subscription_not_found}, state}
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:read_snapshot, source_uuid}, _from, %State{} = state) do
    %State{snapshots: snapshots} = state

    reply =
      case Map.get(snapshots, source_uuid, nil) do
        nil -> {:error, :snapshot_not_found}
        snapshot -> {:ok, deserialize(snapshot, state)}
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:record_snapshot, %SnapshotData{} = snapshot}, _from, %State{} = state) do
    %SnapshotData{source_uuid: source_uuid} = snapshot
    %State{snapshots: snapshots} = state

    state = %State{state | snapshots: Map.put(snapshots, source_uuid, serialize(snapshot, state))}

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:delete_snapshot, source_uuid}, _from, %State{} = state) do
    %State{snapshots: snapshots} = state

    state = %State{state | snapshots: Map.delete(snapshots, source_uuid)}

    {:reply, :ok, state}
  end

  def handle_call(:reset!, _from, %State{} = state) do
    %State{serializer: serializer, persistent_subscriptions: persistent_subscriptions} = state

    for {_name, %PersistentSubscription{subscriptions: subscriptions}} <- persistent_subscriptions do
      for subscription <- subscriptions, is_pid(subscription) do
        :ok = stop_subscription(subscription)
      end
    end

    {:reply, :ok, %State{serializer: serializer}}
  end

  @impl GenServer
  def handle_cast({:ack_event, event, subscriber}, %State{} = state) do
    %State{persistent_subscriptions: persistent_subscriptions} = state

    state = %State{
      state
      | persistent_subscriptions:
          ack_subscription_by_pid(persistent_subscriptions, event, subscriber)
    }

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{} = state) do
    %State{persistent_subscriptions: persistent_subscriptions, transient_subscribers: transient} =
      state

    state = %State{
      state
      | persistent_subscriptions: remove_subscriber_by_pid(persistent_subscriptions, pid),
        transient_subscribers: remove_transient_subscriber_by_pid(transient, pid)
    }

    {:noreply, state}
  end

  defp persist_events(stream_uuid, existing_events, new_events, %State{} = state) do
    %State{
      persisted_events: persisted_events,
      streams: streams,
      next_event_number: next_event_number
    } = state

    initial_stream_version = length(existing_events) + 1
    now = NaiveDateTime.utc_now()

    new_events =
      new_events
      |> Enum.with_index(0)
      |> Enum.map(fn {recorded_event, index} ->
        event_number = next_event_number + index
        stream_version = initial_stream_version + index

        map_to_recorded_event(event_number, stream_uuid, stream_version, now, recorded_event)
      end)
      |> Enum.map(&serialize(&1, state))

    stream_events = prepend(existing_events, new_events)
    next_event_number = List.last(new_events).event_number + 1

    state = %State{
      state
      | streams: Map.put(streams, stream_uuid, stream_events),
        persisted_events: prepend(persisted_events, new_events),
        next_event_number: next_event_number
    }

    publish_all_events = Enum.map(new_events, &deserialize(&1, state))

    publish_to_transient_subscribers(:all, publish_all_events, state)
    publish_to_persistent_subscriptions(:all, publish_all_events, state)

    publish_stream_events = set_event_number_from_version(publish_all_events)

    publish_to_transient_subscribers(stream_uuid, publish_stream_events, state)
    publish_to_persistent_subscriptions(stream_uuid, publish_stream_events, state)

    {:ok, state}
  end

  # Event number should equal stream version for stream events.
  defp set_event_number_from_version(events) do
    Enum.map(events, fn %RecordedEvent{stream_version: stream_version} = event ->
      %RecordedEvent{event | event_number: stream_version}
    end)
  end

  defp prepend(list, []), do: list
  defp prepend(list, [item | remainder]), do: prepend([item | list], remainder)

  defp map_to_recorded_event(event_number, stream_uuid, stream_version, now, %EventData{} = event) do
    %EventData{
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata
    } = event

    %RecordedEvent{
      event_id: UUID.uuid4(),
      event_number: event_number,
      stream_id: stream_uuid,
      stream_version: stream_version,
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      created_at: now
    }
  end

  defp persistent_subscription(
         %PersistentSubscription{} = subscription,
         subscriber,
         %State{} = state
       ) do
    %PersistentSubscription{name: subscription_name, subscriptions: subscriptions} = subscription
    %State{persistent_subscriptions: persistent_subscriptions} = state

    subscription_spec = subscriber |> Subscription.child_spec() |> Map.put(:restart, :temporary)

    {:ok, pid} =
      DynamicSupervisor.start_child(__MODULE__.SubscriptionsSupervisor, subscription_spec)

    Process.monitor(pid)

    subscription = %PersistentSubscription{subscription | subscriptions: [pid | subscriptions]}

    :ok = catch_up(subscription, state)

    state = %State{
      state
      | persistent_subscriptions:
          Map.put(persistent_subscriptions, subscription_name, subscription)
    }

    {{:ok, pid}, state}
  end

  defp stop_subscription(subscription) do
    DynamicSupervisor.terminate_child(__MODULE__.SubscriptionsSupervisor, subscription)
  end

  defp remove_subscriber_by_pid(persistent_subscriptions, pid) do
    case persistent_subscription_by_pid(persistent_subscriptions, pid) do
      {name, %PersistentSubscription{} = subscription} ->
        %PersistentSubscription{subscriptions: subscriptions} = subscription

        subscription = %PersistentSubscription{
          subscription
          | subscriptions: subscriptions -- [pid]
        }

        Map.put(persistent_subscriptions, name, subscription)

      nil ->
        persistent_subscriptions
    end
  end

  defp ack_subscription_by_pid(persistent_subscriptions, %RecordedEvent{} = event, pid) do
    %RecordedEvent{event_number: event_number} = event

    case persistent_subscription_by_pid(persistent_subscriptions, pid) do
      {name, %PersistentSubscription{} = subscription} ->
        subscription = %PersistentSubscription{subscription | last_seen: event_number}

        Map.put(persistent_subscriptions, name, subscription)

      nil ->
        persistent_subscriptions
    end
  end

  defp persistent_subscription_by_pid(persistent_subscriptions, pid) do
    Enum.find(persistent_subscriptions, fn
      {_name, %PersistentSubscription{} = subscription} ->
        %PersistentSubscription{subscriptions: subscriptions} = subscription

        Enum.member?(subscriptions, pid)
    end)
  end

  defp remove_transient_subscriber_by_pid(transient_subscriptions, pid) do
    Enum.reduce(transient_subscriptions, transient_subscriptions, fn
      {stream_uuid, subscribers}, transient ->
        Map.put(transient, stream_uuid, subscribers -- [pid])
    end)
  end

  defp catch_up(%PersistentSubscription{subscriptions: []}, _state), do: :ok
  defp catch_up(%PersistentSubscription{start_from: :current}, _state), do: :ok

  defp catch_up(%PersistentSubscription{stream_uuid: :all} = subscription, %State{} = state) do
    %PersistentSubscription{last_seen: last_seen} = subscription
    %State{persisted_events: persisted_events} = state

    unseen_events =
      persisted_events
      |> Enum.reverse()
      |> Enum.drop(last_seen)
      |> Enum.map(&deserialize(&1, state))

    publish_to_persistent_subscription(unseen_events, subscription)
  end

  defp catch_up(%PersistentSubscription{} = subscription, %State{} = state) do
    %PersistentSubscription{stream_uuid: stream_uuid, last_seen: last_seen} = subscription

    %State{streams: streams} = state

    streams
    |> Map.get(stream_uuid, [])
    |> Enum.reverse()
    |> Enum.drop(last_seen)
    |> Enum.map(&deserialize(&1, state))
    |> set_event_number_from_version()
    |> case do
      [] ->
        :ok

      unseen_events ->
        publish_to_persistent_subscription(unseen_events, subscription)
    end
  end

  defp publish_to_transient_subscribers(stream_uuid, events, %State{} = state) do
    %State{transient_subscribers: transient} = state

    subscribers = Map.get(transient, stream_uuid, [])

    for subscriber <- subscribers |> Enum.filter(&is_pid/1) do
      send(subscriber, {:events, events})
    end
  end

  defp publish_to_persistent_subscriptions(stream_uuid, events, %State{} = state) do
    %State{persistent_subscriptions: persistent_subscriptions} = state

    for {_name, %PersistentSubscription{stream_uuid: ^stream_uuid} = subscription} <-
          persistent_subscriptions do
      publish_to_persistent_subscription(events, subscription)
    end
  end

  defp publish_to_persistent_subscription(events, %PersistentSubscription{} = subscription) do
    %PersistentSubscription{subscriptions: subscriptions} = subscription

    for batch <- Enum.chunk_every(events, length(subscriptions)) do
      batch
      |> Enum.zip(subscriptions)
      |> Enum.each(fn {event, subscription} ->
        send(subscription, {:events, [event]})
      end)
    end

    :ok
  end

  defp serialize(data, %State{serializer: nil}), do: data

  defp serialize(%RecordedEvent{} = recorded_event, %State{} = state) do
    %RecordedEvent{data: data, metadata: metadata} = recorded_event
    %State{serializer: serializer} = state

    %RecordedEvent{
      recorded_event
      | data: serializer.serialize(data),
        metadata: serializer.serialize(metadata)
    }
  end

  defp serialize(%SnapshotData{} = snapshot, %State{} = state) do
    %SnapshotData{data: data, metadata: metadata} = snapshot
    %State{serializer: serializer} = state

    %SnapshotData{
      snapshot
      | data: serializer.serialize(data),
        metadata: serializer.serialize(metadata)
    }
  end

  def deserialize(data, %State{serializer: nil}), do: data

  def deserialize(%RecordedEvent{} = recorded_event, %State{} = state) do
    %RecordedEvent{data: data, metadata: metadata, event_type: event_type} = recorded_event
    %State{serializer: serializer} = state

    %RecordedEvent{
      recorded_event
      | data: serializer.deserialize(data, type: event_type),
        metadata: serializer.deserialize(metadata)
    }
  end

  def deserialize(%SnapshotData{} = snapshot, %State{} = state) do
    %SnapshotData{data: data, metadata: metadata, source_type: source_type} = snapshot
    %State{serializer: serializer} = state

    %SnapshotData{
      snapshot
      | data: serializer.deserialize(data, type: source_type),
        metadata: serializer.deserialize(metadata)
    }
  end
end
