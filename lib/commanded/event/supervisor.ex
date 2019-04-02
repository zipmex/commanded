defmodule Commanded.Event.Handler.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    {concurrency, opts} = Keyword.pop(opts, :concurrency)
    {module, opts} = Keyword.pop(opts, :module)
    {name, opts} = Keyword.pop(opts, :name)

    children =
      for n <- 1..concurrency do
        opts = Keyword.put(opts, :index, n)

        %{
          id: {module, name, n},
          start: {module, :start_link, [opts]},
          restart: :permanent,
          type: :worker
        }
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
