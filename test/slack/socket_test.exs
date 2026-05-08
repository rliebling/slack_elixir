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
