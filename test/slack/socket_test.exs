defmodule Slack.SocketTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Slack.TestBot

  @foo_event """
  {
    "envelope_id": "eid-234",
    "type": "foo",
    "payload": {
      "event": {
        "type": "foo",
        "channel": "channel-foo"
      }
    }
  }
  """

  @slash_command """
  {
    "envelope_id": "eid-567",
    "type": "slash_commands",
    "payload": {
      "channel_name": "directmessage",
      "command": "/mycmd",
      "text": "run this"
    }
  }
  """

  @bot %Slack.Bot{
    id: "bot-123-ABC",
    module: TestBot,
    token: "bot-123-ABC",
    team_id: "team-123-ABC",
    user_id: "user-123-ABC"
  }

  defmodule EnvelopeBot do
    use Slack.Bot

    @impl Slack.Bot
    def handle_event(_type, _payload, _bot) do
      send(self(), :unexpected_event_callback)
    end

    @impl Slack.Bot
    def handle_envelope(envelope, bot) do
      send(self(), {:handled_envelope, envelope, bot})
      {:ack, %{payload: %{"text" => "received"}}}
    end
  end

  setup :set_mimic_global

  setup do
    stub(Slack.API)
    start_supervised!({Registry, keys: :unique, name: Slack.MessageServerRegistry})

    start_supervised!(
      {PartitionSupervisor, child_spec: Task.Supervisor, name: Slack.TaskSupervisors}
    )

    :ok
  end

  test "bot can noop" do
    stub(Slack.API)

    state = %{bot: @bot}

    assert {:reply, ack_frame, _state} = Slack.Socket.handle_frame({:text, @foo_event}, state)
    assert {:text, ~S({"envelope_id":"eid-234"})} = ack_frame
  end

  test "bot can handle full envelopes and control Socket Mode acknowledgments" do
    bot = %{@bot | module: EnvelopeBot}
    state = %{bot: bot}

    assert {:reply, {:text, ack_frame}, new_state} =
             Slack.Socket.handle_frame({:text, @foo_event}, state)

    assert %{bot: ^bot, last_alive_mono: last_alive_mono} = new_state
    assert is_integer(last_alive_mono)

    assert %{"envelope_id" => "eid-234", "payload" => %{"text" => "received"}} =
             Jason.decode!(ack_frame)

    assert_receive {:handled_envelope, %{"envelope_id" => "eid-234"}, ^bot}
    refute_receive :unexpected_event_callback
  end

  test "socket envelope handler can suppress the automatic acknowledgment" do
    handler = fn envelope, state ->
      send(self(), {:handled_envelope, envelope, state.bot})
      :noreply
    end

    state = %{bot: @bot, envelope_handler: handler}

    assert {:ok, new_state} = Slack.Socket.handle_frame({:text, @foo_event}, state)
    assert %{bot: @bot, envelope_handler: ^handler, last_alive_mono: last_alive_mono} = new_state
    assert is_integer(last_alive_mono)
    assert_receive {:handled_envelope, %{"envelope_id" => "eid-234"}, @bot}
  end

  test "socket can noop" do
    stub(Slack.API)

    assert {:ok, state} = Slack.Socket.handle_frame({:text, ""}, %{})
    # handle_frame stamps last_alive_mono; no other keys should be touched.
    assert Map.delete(state, :last_alive_mono) == %{}
    assert is_integer(state.last_alive_mono)
  end

  test "socket can handle a slash command" do
    stub(Slack.API)

    assert {:reply, {:text, ~S({"envelope_id":"eid-567"})}, %{}} =
             Slack.Socket.handle_frame({:text, @slash_command}, %{})
  end

  test "build_state preserves API options for direct socket starts" do
    state =
      Slack.Socket.build_state("xapp-virtual", @bot, api: [base_url: "http://localhost:4000/api"])

    assert state.api_opts == [base_url: "http://localhost:4000/api"]
  end

  test "build_state preserves Socket Mode envelope handlers" do
    handler = fn envelope -> {:ack, envelope} end
    state = Slack.Socket.build_state("xapp-virtual", @bot, envelope_handler: handler)

    assert state.envelope_handler == handler
  end

  describe "server-initiated disconnect frames" do
    for reason <- ["warning", "refresh_requested", "too_many_websockets"] do
      test "disconnect reason=#{reason} triggers a clean close and cancels timers" do
        reason = unquote(reason)

        frame =
          Jason.encode!(%{
            "type" => "disconnect",
            "reason" => reason,
            "debug_info" => %{"host" => "applink-1"}
          })

        state = heartbeat_state()

        assert {:close, new_state} = Slack.Socket.handle_frame({:text, frame}, state)

        assert new_state.heartbeat_ref == nil
        assert new_state.watchdog_ref == nil
      end
    end

    test "disconnect frame with unknown reason still closes" do
      frame =
        Jason.encode!(%{
          "type" => "disconnect",
          "reason" => "something_new"
        })

      state = heartbeat_state()

      assert {:close, new_state} = Slack.Socket.handle_frame({:text, frame}, state)
      assert new_state.heartbeat_ref == nil
      assert new_state.watchdog_ref == nil
    end
  end

  describe "heartbeat" do
    test "handle_info(:heartbeat, _) sends a ping frame and reschedules" do
      state = heartbeat_state()

      assert {:reply, {:ping, "hb"}, new_state} =
               Slack.Socket.handle_info(:heartbeat, state)

      assert is_reference(new_state.heartbeat_ref)
      refute new_state.heartbeat_ref == state.heartbeat_ref
    end

    test "handle_pong/2 stamps last_alive_mono forward" do
      before_mono = System.monotonic_time(:millisecond)
      state = heartbeat_state(last_alive_mono: before_mono - 5_000)

      assert {:ok, new_state} = Slack.Socket.handle_pong(:pong, state)
      assert new_state.last_alive_mono >= before_mono
    end

    test "handle_frame/2 for any text frame stamps last_alive_mono forward" do
      before_mono = System.monotonic_time(:millisecond)
      state = heartbeat_state(last_alive_mono: before_mono - 5_000)

      assert {:ok, new_state} = Slack.Socket.handle_frame({:text, ""}, state)
      assert new_state.last_alive_mono >= before_mono
    end

    test "handle_info(:watchdog, _) reschedules when link is fresh" do
      state = heartbeat_state(last_alive_mono: System.monotonic_time(:millisecond))

      assert {:ok, new_state} = Slack.Socket.handle_info(:watchdog, state)
      assert is_reference(new_state.watchdog_ref)
    end

    test "handle_info(:watchdog, _) closes the socket when link is stale" do
      # last_alive far in the past, well beyond the stale threshold
      stale_mono = System.monotonic_time(:millisecond) - 10_000

      state =
        heartbeat_state(
          last_alive_mono: stale_mono,
          heartbeat_stale_ms: 100
        )

      assert {:close, {4000, "heartbeat stale"}, _new_state} =
               Slack.Socket.handle_info(:watchdog, state)
    end

    test "handle_info(:watchdog, _) does not close when last_alive_mono is nil" do
      # Pre-connect state — no frames ever arrived yet. Don't false-positive.
      state = heartbeat_state(last_alive_mono: nil)

      assert {:ok, _new_state} = Slack.Socket.handle_info(:watchdog, state)
    end

    test "handle_disconnect/2 cancels timers" do
      state = heartbeat_state()

      assert {:ok, new_state} = Slack.Socket.handle_disconnect(%{}, state)
      assert new_state.heartbeat_ref == nil
      assert new_state.watchdog_ref == nil
    end
  end

  # Build a state map consistent with what `start_link/1` produces.
  # Timers start as real references so cancellation paths are exercised.
  defp heartbeat_state(overrides \\ []) do
    defaults = %{
      app_token: "xapp-test",
      bot: @bot,
      heartbeat_interval_ms: 30_000,
      heartbeat_stale_ms: 90_000,
      last_alive_mono: System.monotonic_time(:millisecond),
      heartbeat_ref: Process.send_after(self(), :noop, 60_000),
      watchdog_ref: Process.send_after(self(), :noop, 60_000)
    }

    Enum.reduce(overrides, defaults, fn {k, v}, acc -> Map.put(acc, k, v) end)
  end
end
