defmodule Commanded.Event.EventHandlerConcurrencyTest do
  use Commanded.StorageCase

  alias Commanded.EventStore

  defmodule Event do
    @derive Jason.Encoder
    defstruct [:stream_uuid]
  end

  defmodule ConcurrentEventHandler do
    use Commanded.Event.Handler, name: __MODULE__, concurrency: 5

    def init do
      Process.send(:test, {:init, self()}, [])
    end

    def handle(%Event{} = event, _metadata) do
      %Event{stream_uuid: stream_uuid} = event

      Process.send(:test, {:event, stream_uuid, self()}, [])
    end
  end

  describe "concurrent event handler" do
    setup do
      true = Process.register(self(), :test)

      {:ok, supervisor} =
        Supervisor.start_link([{ConcurrentEventHandler, []}], strategy: :one_for_one)

      [supervisor: supervisor]
    end

    test "should start one child handler per requested concurrency", %{supervisor: supervisor} do
      assert [{_id, handler_supervisor, :supervisor, _modules}] =
               Supervisor.which_children(supervisor)

      assert %{active: 5, specs: 5, supervisors: 0, workers: 5} =
               Supervisor.count_children(handler_supervisor)
    end

    test "should call `init/0` callback once for each started handler" do
      assert_receive {:init, pid1}
      assert_receive {:init, pid2}
      assert_receive {:init, pid3}
      assert_receive {:init, pid4}
      assert_receive {:init, pid5}
      refute_receive {:init, _pid}

      unique_pids = Enum.uniq([pid1, pid2, pid3, pid4, pid5])
      assert length(unique_pids) == 5
    end

    test "should only receive an event once" do
      append_events_to_stream("stream1", 1)

      assert_receive {:event, "stream1", _pid}
      refute_receive {:event, "stream1"}
    end

    test "should distribute events amongst handlers" do
      append_events_to_stream("stream1", 5)

      assert_receive {:event, "stream1", pid1}
      assert_receive {:event, "stream1", pid2}
      assert_receive {:event, "stream1", pid3}
      assert_receive {:event, "stream1", pid4}
      assert_receive {:event, "stream1", pid5}
      refute_receive {:event, "stream1"}

      unique_pids = Enum.uniq([pid1, pid2, pid3, pid4, pid5])
      assert length(unique_pids) == 5
    end
  end

  defp append_events_to_stream(stream_uuid, count) do
    events =
      1..count
      |> Enum.map(fn _i -> %Event{stream_uuid: stream_uuid} end)
      |> Commanded.Event.Mapper.map_to_event_data([])

    EventStore.append_to_stream(stream_uuid, :any_version, events)
  end
end
