defmodule Commanded.EventStore.SubscriptionTestCase do
  import Commanded.SharedTestCase

  define_tests do
    alias Commanded.EventStore
    alias Commanded.EventStore.{EventData, Subscriber}
    alias Commanded.Helpers.ProcessHelper

    defmodule BankAccountOpened do
      @derive Jason.Encoder
      defstruct [:account_number, :initial_balance]
    end

    describe "transient subscription to single stream" do
      test "should receive events appended to the stream" do
        stream_uuid = UUID.uuid4()

        assert :ok = EventStore.subscribe(stream_uuid)

        :ok = EventStore.append_to_stream(stream_uuid, 0, build_events(1))

        received_events = assert_receive_events(expected_count: 1, from: 1)
        assert Enum.map(received_events, & &1.stream_id) == [stream_uuid]
        assert Enum.map(received_events, & &1.stream_version) == [1]

        :ok = EventStore.append_to_stream(stream_uuid, 1, build_events(2))

        received_events = assert_receive_events(expected_count: 2, from: 2)
        assert Enum.map(received_events, & &1.stream_id) == [stream_uuid, stream_uuid]
        assert Enum.map(received_events, & &1.stream_version) == [2, 3]

        :ok = EventStore.append_to_stream(stream_uuid, 3, build_events(3))

        received_events = assert_receive_events(expected_count: 3, from: 4)

        assert Enum.map(received_events, & &1.stream_id) == [
                 stream_uuid,
                 stream_uuid,
                 stream_uuid
               ]

        assert Enum.map(received_events, & &1.stream_version) == [4, 5, 6]

        refute_receive {:events, _received_events}
      end

      test "should not receive events appended to another stream" do
        stream_uuid = UUID.uuid4()
        another_stream_uuid = UUID.uuid4()

        assert :ok = EventStore.subscribe(stream_uuid)

        :ok = EventStore.append_to_stream(another_stream_uuid, 0, build_events(1))
        :ok = EventStore.append_to_stream(another_stream_uuid, 1, build_events(2))

        refute_receive {:events, _received_events}
      end
    end

    describe "transient subscription to all streams" do
      test "should receive events appended to any stream" do
        assert :ok = EventStore.subscribe(:all)

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))

        received_events = assert_receive_events(expected_count: 1, from: 1)
        assert Enum.map(received_events, & &1.stream_id) == ["stream1"]
        assert Enum.map(received_events, & &1.stream_version) == [1]

        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))

        received_events = assert_receive_events(expected_count: 2, from: 2)
        assert Enum.map(received_events, & &1.stream_id) == ["stream2", "stream2"]
        assert Enum.map(received_events, & &1.stream_version) == [1, 2]

        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        received_events = assert_receive_events(expected_count: 3, from: 4)
        assert Enum.map(received_events, & &1.stream_id) == ["stream3", "stream3", "stream3"]
        assert Enum.map(received_events, & &1.stream_version) == [1, 2, 3]

        :ok = EventStore.append_to_stream("stream1", 1, build_events(2))

        received_events = assert_receive_events(expected_count: 2, from: 7)
        assert Enum.map(received_events, & &1.stream_id) == ["stream1", "stream1"]
        assert Enum.map(received_events, & &1.stream_version) == [2, 3]

        refute_receive {:events, _received_events}
      end
    end

    describe "persistent subscription to single stream" do
      test "should receive `:subscribed` message once subscribed" do
        {:ok, subscription} =
          EventStore.subscribe_to("stream1", "subscriber", self(), start_from: :origin)

        assert_receive {:subscribed, ^subscription}
      end

      test "should receive events appended to stream" do
        {:ok, subscription} =
          EventStore.subscribe_to("stream1", "subscriber", self(), start_from: :origin)

        assert_receive {:subscribed, ^subscription}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream1", 1, build_events(2))
        :ok = EventStore.append_to_stream("stream1", 3, build_events(3))

        assert_receive_events(subscription, expected_count: 1, from: 1)
        assert_receive_events(subscription, expected_count: 2, from: 2)
        assert_receive_events(subscription, expected_count: 3, from: 4)

        refute_receive {:events, _received_events}
      end

      test "should not receive events appended to another stream" do
        {:ok, subscription} =
          EventStore.subscribe_to("stream1", "subscriber", self(), start_from: :origin)

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))
        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        assert_receive_events(subscription, expected_count: 1, from: 1)
        refute_receive {:events, _received_events}
      end

      test "should skip existing events when subscribing from current position" do
        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream1", 1, build_events(2))

        wait_for_event_store()

        {:ok, subscription} =
          EventStore.subscribe_to("stream1", "subscriber", self(), start_from: :current)

        assert_receive {:subscribed, ^subscription}
        refute_receive {:events, _events}

        :ok = EventStore.append_to_stream("stream1", 3, build_events(3))
        :ok = EventStore.append_to_stream("stream2", 0, build_events(3))
        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        assert_receive_events(subscription, expected_count: 3, from: 4)
        refute_receive {:events, _events}
      end

      test "should receive events already apended to stream" do
        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))
        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        {:ok, subscription} =
          EventStore.subscribe_to("stream3", "subscriber", self(), start_from: :origin)

        assert_receive {:subscribed, ^subscription}

        assert_receive_events(subscription, expected_count: 3, from: 1)

        :ok = EventStore.append_to_stream("stream3", 3, build_events(1))
        :ok = EventStore.append_to_stream("stream3", 4, build_events(1))

        assert_receive_events(subscription, expected_count: 2, from: 4)
        refute_receive {:events, _received_events}
      end

      test "should prevent duplicate subscriptions" do
        {:ok, _subscription} =
          EventStore.subscribe_to("stream1", "subscriber", self(), start_from: :origin)

        assert {:error, :already_subscribed} ==
                 EventStore.subscribe_to("stream1", "subscriber", self(), start_from: :origin)
      end
    end

    describe "persistent subscription to all streams" do
      test "should receive `:subscribed` message once subscribed" do
        {:ok, subscription} = subscribe_to_all()

        assert_receive {:subscribed, ^subscription}
      end

      test "should receive events appended to any stream" do
        {:ok, subscription} = subscribe_to_all()

        assert_receive {:subscribed, ^subscription}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))
        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        assert_receive_events(subscription, expected_count: 1, from: 1)
        assert_receive_events(subscription, expected_count: 2, from: 2)
        assert_receive_events(subscription, expected_count: 3, from: 4)

        refute_receive {:events, _received_events}
      end

      test "should receive events already appended to any stream" do
        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))

        wait_for_event_store()

        {:ok, subscription} = subscribe_to_all()

        assert_receive {:subscribed, ^subscription}

        assert_receive_events(subscription, expected_count: 1, from: 1)
        assert_receive_events(subscription, expected_count: 2, from: 2)

        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        assert_receive_events(subscription, expected_count: 3, from: 4)
        refute_receive {:events, _received_events}
      end

      test "should skip existing events when subscribing from current position" do
        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))

        wait_for_event_store()

        {:ok, subscription} =
          EventStore.subscribe_to(:all, "subscriber", self(), start_from: :current)

        assert_receive {:subscribed, ^subscription}
        refute_receive {:events, _received_events}

        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        assert_receive_events(subscription, expected_count: 3, from: 4)

        refute_receive {:events, _received_events}
      end

      test "should prevent duplicate subscriptions" do
        {:ok, _subscription} = subscribe_to_all()

        assert {:error, :already_subscribed} == subscribe_to_all()
      end
    end

    describe "persistent subscription concurrency" do
      test "should allow multiple subscribers to single subscription" do
        {:ok, _subscriber1} = Subscriber.start_link(self(), concurrency: 2)
        {:ok, _subscriber2} = Subscriber.start_link(self(), concurrency: 2)

        assert_receive {:subscribed, _subscription1}
        assert_receive {:subscribed, _subscription2}
        refute_receive {:subscribed, _subscription}
      end

      test "should prevent too many subscribers to single subscription" do
        {:ok, _subscriber} = Subscriber.start_link(self(), concurrency: 1)

        assert {:error, :too_many_subscribers} = Subscriber.start(self(), concurrency: 1)
      end

      test "should prevent too many subscribers to subscription with concurrency" do
        {:ok, _subscriber1} = Subscriber.start_link(self(), concurrency: 3)
        {:ok, _subscriber2} = Subscriber.start_link(self(), concurrency: 3)
        {:ok, _subscriber3} = Subscriber.start_link(self(), concurrency: 3)

        assert {:error, :too_many_subscribers} = Subscriber.start(self(), concurrency: 3)
      end

      test "should distribute events amongst subscribers" do
        {:ok, _subscriber1} = Subscriber.start_link(self(), concurrency: 2)
        {:ok, _subscriber2} = Subscriber.start_link(self(), concurrency: 2)

        assert_receive {:subscribed, subscription1}
        assert_receive {:subscribed, subscription2}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(4))

        assert_receive_events(subscription1, expected_count: 1, from: 1)
        assert_receive_events(subscription2, expected_count: 1, from: 2)
        assert_receive_events(subscription1, expected_count: 1, from: 3)
        assert_receive_events(subscription2, expected_count: 1, from: 4)

        refute_receive {:events, _subscription, _received_events}
      end

      test "should exclude stopped subscriber from receiving events" do
        {:ok, subscriber1} = Subscriber.start_link(self(), concurrency: 2)
        {:ok, subscriber2} = Subscriber.start_link(self(), concurrency: 2)

        assert_receive {:subscribed, subscription1}
        assert_receive {:subscribed, subscription2}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(2))

        assert_receive_events(subscription1, expected_count: 1, from: 1)
        assert_receive_events(subscription2, expected_count: 1, from: 2)

        stop_subscriber(subscriber1)

        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))

        assert_receive_events(subscription2, expected_count: 1, from: 3)
        assert_receive_events(subscription2, expected_count: 1, from: 4)

        stop_subscriber(subscriber2)

        :ok = EventStore.append_to_stream("stream3", 0, build_events(2))

        refute_receive {:events, _subscription, _received_events}
      end
    end

    describe "unsubscribe from all streams" do
      test "should not receive further events appended to any stream" do
        {:ok, subscription} = subscribe_to_all()

        assert_receive {:subscribed, ^subscription}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))

        assert_receive_events(subscription, expected_count: 1, from: 1)

        :ok = unsubscribe(subscription)

        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))
        :ok = EventStore.append_to_stream("stream3", 0, build_events(3))

        refute_receive {:events, _received_events}
      end

      test "should resume subscription when subscribing again" do
        {:ok, subscription1} = subscribe_to_all()

        assert_receive {:subscribed, ^subscription1}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))

        assert_receive_events(subscription1, expected_count: 1, from: 1)

        :ok = unsubscribe(subscription1)

        {:ok, subscription2} = subscribe_to_all()

        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))

        assert_receive {:subscribed, ^subscription2}
        assert_receive_events(subscription2, expected_count: 2, from: 2)
      end
    end

    describe "delete subscription" do
      test "should be deleted" do
        {:ok, subscription1} = subscribe_to_all()

        assert_receive {:subscribed, ^subscription1}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))

        assert_receive_events(subscription1, expected_count: 1, from: 1)

        :ok = unsubscribe(subscription1)

        assert :ok = EventStore.delete_subscription(:all, "subscriber")
      end

      test "should create new subscription after deletion" do
        {:ok, subscription1} = subscribe_to_all()

        assert_receive {:subscribed, ^subscription1}

        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))

        assert_receive_events(subscription1, expected_count: 1, from: 1)

        :ok = unsubscribe(subscription1)

        :ok = EventStore.delete_subscription(:all, "subscriber")

        :ok = EventStore.append_to_stream("stream2", 0, build_events(2))

        refute_receive {:events, _received_events}

        {:ok, subscription2} = subscribe_to_all()

        # Should receive all events as subscription has been recreated from `:origin`
        assert_receive {:subscribed, ^subscription2}
        assert_receive_events(subscription2, expected_count: 1, from: 1)
        assert_receive_events(subscription2, expected_count: 2, from: 2)
      end
    end

    describe "resume subscription" do
      test "should remember last seen event number when subscription resumes" do
        :ok = EventStore.append_to_stream("stream1", 0, build_events(1))
        :ok = EventStore.append_to_stream("stream2", 0, build_events(1))

        {:ok, subscriber} = Subscriber.start_link(self())

        assert_receive {:subscribed, subscription}
        assert_receive {:events, ^subscription, received_events}
        assert length(received_events) == 1
        assert Enum.map(received_events, & &1.stream_id) == ["stream1"]

        :ok = EventStore.ack_event(subscription, List.last(received_events))

        assert_receive {:events, ^subscription, received_events}
        assert length(received_events) == 1
        assert Enum.map(received_events, & &1.stream_id) == ["stream2"]

        :ok = EventStore.ack_event(subscription, List.last(received_events))

        stop_subscriber(subscriber)

        {:ok, _subscriber} = Subscriber.start_link(self())

        assert_receive {:subscribed, subscription}

        :ok = EventStore.append_to_stream("stream3", 0, build_events(1))

        assert_receive {:events, ^subscription, received_events}
        assert length(received_events) == 1
        assert Enum.map(received_events, & &1.stream_id) == ["stream3"]

        :ok = EventStore.ack_event(subscription, List.last(received_events))

        refute_receive {:events, _subscription, _received_events}
      end
    end

    describe "subscription process" do
      test "should not stop subscriber process when subscription down" do
        {:ok, subscriber} = Subscriber.start_link(self())

        ref = Process.monitor(subscriber)

        assert_receive {:subscribed, subscription}

        ProcessHelper.shutdown(subscription)

        refute Process.alive?(subscription)
        assert Process.alive?(subscriber)
        refute_receive {:DOWN, ^ref, :process, ^subscriber, _reason}
      end

      test "should stop subscription process when subscriber down" do
        {:ok, subscriber} = Subscriber.start_link(self())

        assert_receive {:subscribed, subscription}

        ref = Process.monitor(subscription)

        stop_subscriber(subscriber)

        assert_receive {:DOWN, ^ref, :process, ^subscription, _reason}
      end
    end

    defp subscribe_to_all(opts \\ []) do
      EventStore.subscribe_to(:all, "subscriber", self(), opts)
    end

    defp unsubscribe(subscription) do
      :ok = EventStore.unsubscribe(subscription)

      wait_for_event_store()
    end

    defp stop_subscriber(subscriber) do
      ProcessHelper.shutdown(subscriber)

      wait_for_event_store()
    end

    # Optionally wait for the event store
    defp wait_for_event_store do
      case event_store_wait() do
        nil -> :ok
        wait -> :timer.sleep(wait)
      end
    end

    defp assert_receive_events(subscription, opts) do
      assert_receive_events(Keyword.put(opts, :subscription, subscription))
    end

    defp assert_receive_events(opts) do
      expected_count = Keyword.get(opts, :expected_count, 1)
      from_event_number = Keyword.get(opts, :from, 1)

      received_events =
        case Keyword.get(opts, :subscription) do
          nil -> receive_transient_events(from_event_number)
          subscription -> receive_persistent_events(from_event_number, subscription)
        end

      case expected_count - length(received_events) do
        0 ->
          received_events

        remaining when remaining > 0 ->
          opts =
            opts
            |> Keyword.put(:from, from_event_number + length(received_events))
            |> Keyword.put(:expected_count, remaining)

          received_events ++ assert_receive_events(opts)

        remaining when remaining < 0 ->
          flunk("Received #{abs(remaining)} more event(s) than expected")
      end
    end

    defp receive_transient_events(from_event_number) do
      assert_receive {:events, received_events}

      assert_received_events(received_events, from_event_number)

      received_events
    end

    defp receive_persistent_events(
           from_event_number,
           subscription,
           timeout \\ Application.fetch_env!(:ex_unit, :assert_receive_timeout)
         ) do
      received_events =
        receive do
          {:events, received_events} -> received_events
          {:events, ^subscription, received_events} -> received_events
        after
          timeout -> flunk("No events received after #{timeout}ms.")
        end

      assert_received_events(received_events, from_event_number)

      :ok = EventStore.ack_event(subscription, List.last(received_events))

      received_events
    end

    defp assert_received_events(received_events, from_event_number) do
      received_events
      |> Enum.with_index(from_event_number)
      |> Enum.each(fn {received_event, expected_event_number} ->
        assert received_event.event_number == expected_event_number
      end)
    end

    defp build_event(account_number) do
      %EventData{
        causation_id: UUID.uuid4(),
        correlation_id: UUID.uuid4(),
        event_type: "#{__MODULE__}.BankAccountOpened",
        data: %BankAccountOpened{account_number: account_number, initial_balance: 1_000},
        metadata: %{"user_id" => "test"}
      }
    end

    defp build_events(count) do
      for account_number <- 1..count, do: build_event(account_number)
    end
  end
end
