defmodule Slack.SocketManager do
  @moduledoc false
  # Owns the lifecycle of a `Slack.Socket` WebSocket and the reconnect
  # policy around it.
  #
  # Why a GenServer and not a Supervisor
  # ------------------------------------
  # OTP's `Supervisor` behaviour restarts children immediately with no
  # state between attempts. Slack Socket Mode requires us to:
  #
  #   * call `apps.connections.open` (a blocking HTTP request) to fetch a
  #     fresh WebSocket URL on every attempt, which can fail for
  #     environmental reasons (network down, DNS, TLS, Slack 5xx);
  #   * wait a growing, jittered delay between attempts so a long
  #     outage doesn't hammer Slack's API;
  #   * keep `attempt` state across reconnects so the delay actually
  #     grows;
  #   * treat "socket process died" and "couldn't open socket at all"
  #     as the same event, funneling both into the same backoff loop.
  #
  # None of that fits inside a `Supervisor` child_spec. This module is
  # therefore a GenServer that manages a single `Slack.Socket` process
  # as a non-supervised linked child: it `WebSockex.start_link/3`s the
  # socket, `Process.monitor/1`s it, and schedules a reconnect on every
  # `:DOWN`.
  #
  # Backoff schedule
  # ----------------
  # Exponential: 1 s, 2 s, 4 s, 8 s, 16 s, 32 s, then capped at 60 s.
  # Each delay is jittered by ±20 % so multiple clients sharing a
  # network path don't all reconnect on exactly the same tick. The
  # attempt counter resets to 0 whenever `apps.connections.open` +
  # `WebSockex.start_link` both succeed for the current attempt.
  #
  # This GenServer itself is meant to never crash during network
  # outages — its only job is to wait and retry, which doesn't raise.
  # If it *does* crash for a pathological reason (OOM, etc.) its parent
  # supervisor restarts it and it immediately begins a fresh attempt.
  use GenServer

  require Logger

  @base_delay_ms 1_000
  @max_delay_ms 60_000
  @jitter_ratio 0.2

  @typedoc false
  @type state :: %{
          app_token: String.t(),
          bot: Slack.Bot.t(),
          socket_opts: keyword(),
          attempt: non_neg_integer(),
          socket_pid: pid() | nil,
          monitor_ref: reference() | nil,
          reconnect_timer: reference() | nil,
          api_opts: keyword(),
          open_fun: (String.t() -> {:ok, map()} | {:error, term()}),
          start_socket_fun: (String.t(), map() -> {:ok, pid()} | {:error, term()}),
          rand_fun: (-> float()),
          base_delay_ms: pos_integer(),
          max_delay_ms: pos_integer(),
          jitter_ratio: float()
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the socket manager.

  Accepts the same `{app_token, bot, opts}` tuple shape the supervisor
  hands to `Slack.Socket`, plus any additional options used by the
  manager itself (primarily useful in tests):

    * `:open_fun` — 1-arity function returning `{:ok, %{"url" => url}}`
      or `{:error, reason}`. Defaults to
      `&Slack.API.post("apps.connections.open", &1)`.
    * `:api` — API options passed to the default `apps.connections.open`
      request, such as `base_url: "http://localhost:4000/api"`.
    * `:start_socket_fun` — 2-arity function returning `{:ok, pid}` or
      `{:error, reason}`. Defaults to
      `&WebSockex.start_link(&1, Slack.Socket, &2)`.
    * `:rand_fun` — 0-arity function returning a float in [0.0, 1.0).
      Defaults to `&:rand.uniform/0` – 1.0 to give that range. Tests can
      stub it to make jitter deterministic.
    * `:base_delay_ms`, `:max_delay_ms`, `:jitter_ratio` — override the
      backoff schedule constants. Intended for tests.
  """
  @spec start_link({String.t(), Slack.Bot.t(), keyword()}) :: GenServer.on_start()
  def start_link({app_token, bot, opts}) do
    GenServer.start_link(__MODULE__, {app_token, bot, opts})
  end

  def start_link({app_token, bot}), do: start_link({app_token, bot, []})

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]},
      type: :worker,
      restart: :permanent
    }
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init({app_token, bot, opts}) do
    {manager_opts, socket_opts} = split_opts(opts)

    state = %{
      app_token: app_token,
      bot: bot,
      socket_opts: socket_opts,
      attempt: 0,
      socket_pid: nil,
      monitor_ref: nil,
      reconnect_timer: nil,
      api_opts: Keyword.get(manager_opts, :api, []),
      open_fun: open_fun(manager_opts),
      start_socket_fun: Keyword.get(manager_opts, :start_socket_fun, &default_start_socket/2),
      rand_fun: Keyword.get(manager_opts, :rand_fun, &:rand.uniform/0),
      base_delay_ms: Keyword.get(manager_opts, :base_delay_ms, @base_delay_ms),
      max_delay_ms: Keyword.get(manager_opts, :max_delay_ms, @max_delay_ms),
      jitter_ratio: Keyword.get(manager_opts, :jitter_ratio, @jitter_ratio)
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state), do: do_connect(state)

  @impl true
  def handle_info(:reconnect, state) do
    do_connect(%{state | reconnect_timer: nil})
  end

  def handle_info(
        {:DOWN, ref, :process, pid, reason},
        %{monitor_ref: ref, socket_pid: pid} = state
      ) do
    Logger.warning("[Slack.SocketManager] socket down: #{inspect(reason)}; scheduling reconnect")

    state =
      %{state | socket_pid: nil, monitor_ref: nil}
      |> schedule_reconnect()

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    # Stale DOWN for a socket we no longer track.
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call(:get_state, _from, state) do
    reply = %{
      attempt: state.attempt,
      socket_pid: state.socket_pid,
      reconnecting?: is_reference(state.reconnect_timer)
    }

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if is_reference(state.reconnect_timer) do
      _ = Process.cancel_timer(state.reconnect_timer)
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Connection lifecycle
  # ---------------------------------------------------------------------------

  defp do_connect(state) do
    with {:ok, %{"url" => url}} <- state.open_fun.(state.app_token) do
      socket_state =
        Slack.Socket.build_state(state.app_token, state.bot, state.socket_opts)

      case state.start_socket_fun.(url, socket_state) do
        {:ok, pid} when is_pid(pid) ->
          Logger.info("[Slack.SocketManager] socket connected (attempt=#{state.attempt})")
          ref = Process.monitor(pid)

          {:noreply,
           %{state | socket_pid: pid, monitor_ref: ref, attempt: 0, reconnect_timer: nil}}

        {:error, reason} ->
          Logger.warning(
            "[Slack.SocketManager] WebSockex.start_link failed (attempt=#{state.attempt}): #{inspect(reason)}"
          )

          {:noreply, schedule_reconnect(state)}
      end
    else
      {:error, reason} ->
        Logger.warning(
          "[Slack.SocketManager] apps.connections.open failed (attempt=#{state.attempt}): #{inspect(reason)}"
        )

        {:noreply, schedule_reconnect(state)}

      other ->
        Logger.warning(
          "[Slack.SocketManager] apps.connections.open unexpected return (attempt=#{state.attempt}): #{inspect(other)}"
        )

        {:noreply, schedule_reconnect(state)}
    end
  end

  defp schedule_reconnect(state) do
    delay = next_delay(state)
    timer = Process.send_after(self(), :reconnect, delay)

    Logger.info("[Slack.SocketManager] reconnecting in #{delay}ms (attempt=#{state.attempt + 1})")

    %{state | attempt: state.attempt + 1, reconnect_timer: timer}
  end

  # ---------------------------------------------------------------------------
  # Backoff math
  # ---------------------------------------------------------------------------

  defp next_delay(state) do
    # Exponential base capped at max_delay_ms, then jittered by ±ratio.
    base = min(state.base_delay_ms * Integer.pow(2, state.attempt), state.max_delay_ms)
    jitter_span = trunc(base * state.jitter_ratio)
    # rand_fun() returns a float in [0, 1); shift to [-1, 1) and scale.
    offset = trunc((state.rand_fun.() * 2 - 1) * jitter_span)
    max(0, base + offset)
  end

  # ---------------------------------------------------------------------------
  # Option plumbing
  # ---------------------------------------------------------------------------

  @manager_keys [
    :open_fun,
    :start_socket_fun,
    :rand_fun,
    :base_delay_ms,
    :max_delay_ms,
    :jitter_ratio,
    :api
  ]

  defp split_opts(opts) do
    Keyword.split(opts, @manager_keys)
  end

  defp open_fun(manager_opts) do
    Keyword.get_lazy(manager_opts, :open_fun, fn ->
      api_opts = Keyword.get(manager_opts, :api, [])
      fn app_token -> default_open(app_token, api_opts) end
    end)
  end

  defp default_open(app_token, api_opts) do
    api_post("apps.connections.open", app_token, %{}, api_opts)
  end

  defp api_post(endpoint, token, args, []), do: Slack.API.post(endpoint, token, args)
  defp api_post(endpoint, token, args, opts), do: Slack.API.post(endpoint, token, args, opts)

  defp default_start_socket(url, socket_state) do
    WebSockex.start_link(url, Slack.Socket, socket_state)
  end
end
