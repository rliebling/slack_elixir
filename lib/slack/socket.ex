defmodule Slack.Socket do
  @moduledoc false
  # Slack websocket connection for "Socket Mode."
  #
  # Application-level heartbeat
  # ---------------------------
  # A WebSocket connection can go half-open (no FIN/RST ever arrives) and
  # silently stop carrying frames while the kernel still thinks the TCP
  # socket is alive. To detect that, this module runs two independent
  # timers:
  #
  #   * `:heartbeat` — every `heartbeat_interval_ms` we send a protocol
  #     ping frame (`{:ping, "hb"}`). Slack's server is required by the
  #     WebSocket spec to reply with a pong; `handle_pong/2` stamps
  #     `last_alive_mono`.
  #   * `:watchdog` — every `heartbeat_interval_ms` we check whether any
  #     inbound frame (pong or otherwise) has arrived within
  #     `heartbeat_stale_ms`. If not, we close the socket with status
  #     4000 so the supervisor can restart us and fetch a fresh URL via
  #     `apps.connections.open`.
  #
  # Any incoming frame resets `last_alive_mono`, not just pongs — a busy
  # connection that is delivering events proves it is alive even if a
  # single pong is lost.
  #
  # The two knobs default to 30 s interval / 90 s stale threshold. They
  # can be overridden by passing a third element to `start_link/1`:
  #
  #     {Slack.Socket, {app_token, bot,
  #       heartbeat_interval_ms: 15_000,
  #       heartbeat_stale_ms: 45_000}}
  use WebSockex

  require Logger

  @default_heartbeat_interval_ms 30_000
  @default_heartbeat_stale_ms 90_000

  # ----------------------------------------------------------------------------
  # Public API
  # ----------------------------------------------------------------------------

  def start_link({app_token, bot}), do: start_link({app_token, bot, []})

  def start_link({app_token, bot, opts}) when is_list(opts) do
    heartbeat_interval_ms =
      Keyword.get(opts, :heartbeat_interval_ms, @default_heartbeat_interval_ms)

    heartbeat_stale_ms =
      Keyword.get(opts, :heartbeat_stale_ms, @default_heartbeat_stale_ms)

    state = %{
      app_token: app_token,
      bot: bot,
      heartbeat_interval_ms: heartbeat_interval_ms,
      heartbeat_stale_ms: heartbeat_stale_ms,
      last_alive_mono: nil,
      heartbeat_ref: nil,
      watchdog_ref: nil
    }

    {:ok, %{"url" => url}} = Slack.API.post("apps.connections.open", state.app_token)

    Logger.info("[Slack.Socket] connecting...")

    WebSockex.start_link(url, __MODULE__, state)
  end

  # ----------------------------------------------------------------------------
  # Callbacks
  # ----------------------------------------------------------------------------

  @impl WebSockex
  def handle_connect(_conn, state) do
    Logger.info(
      "[Slack.Socket] heartbeat armed: interval=#{state.heartbeat_interval_ms}ms stale_after=#{state.heartbeat_stale_ms}ms"
    )

    state =
      state
      |> mark_alive()
      |> arm_heartbeat()
      |> arm_watchdog()

    {:ok, state}
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    state = mark_alive(state)

    case Jason.decode(msg) do
      {:ok, %{"type" => "hello"} = hello} ->
        Logger.info("[Slack.Socket] hello: #{inspect(hello)}")
        {:ok, state}

      {:ok, %{"payload" => %{"event" => event}} = msg} ->
        Logger.debug("[Slack.Socket] message: #{inspect(msg)}")

        Task.Supervisor.start_child(
          {:via, PartitionSupervisor, {Slack.TaskSupervisors, self()}},
          fn -> handle_slack_event(event["type"], event, state.bot) end
        )

        {:reply, ack_frame(msg), state}

      {:ok, %{"type" => "slash_commands", "payload" => payload} = msg} ->
        Logger.debug("[Slack.Socket] message: #{inspect(msg)}")

        Task.Supervisor.start_child(
          {:via, PartitionSupervisor, {Slack.TaskSupervisors, self()}},
          fn -> handle_slack_event(msg["type"], payload, state.bot) end
        )

        {:reply, ack_frame(msg), state}

      _ ->
        Logger.debug("[Slack.Socket] Unhandled payload: #{msg}")
        {:ok, state}
    end
  end

  @impl WebSockex
  def handle_frame({type, msg}, state) do
    Logger.debug("[Slack.Socket] unhandled message type: #{inspect(type)}, msg: #{inspect(msg)}")
    {:ok, mark_alive(state)}
  end

  @impl WebSockex
  def handle_pong(_frame, state) do
    {:ok, mark_alive(state)}
  end

  @impl WebSockex
  def handle_info(:heartbeat, state) do
    state = arm_heartbeat(state)
    {:reply, {:ping, "hb"}, state}
  end

  def handle_info(:watchdog, state) do
    last_alive = Map.get(state, :last_alive_mono)
    stale_ms = Map.get(state, :heartbeat_stale_ms, @default_heartbeat_stale_ms)
    elapsed = monotonic_ms() - (last_alive || 0)

    if last_alive && elapsed > stale_ms do
      Logger.warning(
        "[Slack.Socket] heartbeat stale: no inbound frame in #{elapsed}ms (threshold #{stale_ms}ms); closing socket"
      )

      {:close, {4000, "heartbeat stale"}, cancel_timers(state)}
    else
      {:ok, arm_watchdog(state)}
    end
  end

  @impl WebSockex
  def handle_disconnect(_status, state) do
    {:ok, cancel_timers(state)}
  end

  @impl WebSockex
  def handle_cast({:send, {type, msg} = frame}, state) do
    Logger.debug("[Slack.Socket] sending #{type} frame with payload: #{msg}")
    {:reply, frame, state}
  end

  @impl WebSockex
  def terminate(_reason, state) do
    _ = cancel_timers(state)
    :ok
  end

  # ----------------------------------------------------------------------------
  # Helpers
  # ----------------------------------------------------------------------------

  # In the case the bot user has JOINED a channel, we need to handle this as a
  # special case.
  defp handle_slack_event(
         "member_joined_channel" = type,
         %{"user" => user} = event,
         %{user_id: user} = bot
       ) do
    Logger.debug("[Slack.Socket] member_joined_channel")
    handle_bot_joined(event, bot)
    bot.module.handle_event(type, event, bot)
  end

  # In the case the bot user has PARTED a channel, we need to handle this as a
  # special case.
  defp handle_slack_event("channel_left" = type, event, bot) do
    Logger.debug("[Slack.Socket] channel_left")
    handle_parted(event, bot)
    bot.module.handle_event(type, event, bot)
  end

  # Ignore messages from yourself...
  defp handle_slack_event("message", %{"user" => user}, %{user_id: user}), do: :ok
  defp handle_slack_event("message", %{"bot_id" => bot_id}, %{bot_id: bot_id}), do: :ok

  # Catch-all case, fall through to bot handler only.
  defp handle_slack_event(type, event, bot) do
    Logger.debug("[Slack.Socket] Sending #{type} event to #{bot.module}")
    bot.module.handle_event(type, event, bot)
  end

  defp handle_bot_joined(%{"channel" => channel} = _event, bot) do
    Slack.ChannelServer.join(bot, channel)
  end

  defp handle_parted(%{"channel" => channel} = _event, bot) do
    Slack.ChannelServer.part(bot, channel)
  end

  defp ack_frame(payload) do
    ack =
      payload
      |> Map.take(["envelope_id"])
      |> Jason.encode!()

    {:text, ack}
  end

  # ----------------------------------------------------------------------------
  # Heartbeat helpers
  # ----------------------------------------------------------------------------

  defp mark_alive(state), do: Map.put(state, :last_alive_mono, monotonic_ms())

  defp arm_heartbeat(state) do
    state = cancel_timer(state, :heartbeat_ref)
    ref = Process.send_after(self(), :heartbeat, state.heartbeat_interval_ms)
    Map.put(state, :heartbeat_ref, ref)
  end

  defp arm_watchdog(state) do
    state = cancel_timer(state, :watchdog_ref)
    ref = Process.send_after(self(), :watchdog, state.heartbeat_interval_ms)
    Map.put(state, :watchdog_ref, ref)
  end

  defp cancel_timers(state) do
    state
    |> cancel_timer(:heartbeat_ref)
    |> cancel_timer(:watchdog_ref)
  end

  defp cancel_timer(state, key) do
    case Map.get(state, key) do
      ref when is_reference(ref) ->
        _ = Process.cancel_timer(ref)
        Map.put(state, key, nil)

      _ ->
        state
    end
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end
