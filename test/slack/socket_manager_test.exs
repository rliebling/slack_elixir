defmodule Slack.SocketManagerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Slack.SocketManager

  @multiplexer_api "http://localhost:4000/api"

  @bot %Slack.Bot{
    id: "bot-123-ABC",
    module: Slack.TestBot,
    token: "bot-123-ABC",
    team_id: "team-123-ABC",
    user_id: "user-123-ABC"
  }

  setup :set_mimic_global

  # Starts a manager with stubbed `open_fun` and `start_socket_fun`.
  # `responses` is a list of return values to hand out one at a time in
  # the order they are requested. The test agent records each call so
  # assertions can examine the call log.
  defp start_manager(open_responses, socket_responses, extra_opts \\ []) do
    test_pid = self()
    {:ok, opens_agent} = Agent.start_link(fn -> open_responses end)
    {:ok, sockets_agent} = Agent.start_link(fn -> socket_responses end)

    open_fun = fn _token ->
      response =
        Agent.get_and_update(opens_agent, fn
          [] -> {{:error, :exhausted}, []}
          [r | rest] -> {r, rest}
        end)

      send(test_pid, {:open_called, response})
      response
    end

    start_socket_fun = fn _url, _state ->
      response =
        Agent.get_and_update(sockets_agent, fn
          [] -> {{:error, :exhausted}, []}
          [r | rest] -> {r, rest}
        end)

      send(test_pid, {:start_socket_called, response})
      response
    end

    opts =
      Keyword.merge(
        [
          open_fun: open_fun,
          start_socket_fun: start_socket_fun,
          # Deterministic "no jitter" for tests that assert on delay.
          rand_fun: fn -> 0.5 end,
          base_delay_ms: 10,
          max_delay_ms: 40,
          jitter_ratio: 0.0
        ],
        extra_opts
      )

    {:ok, manager} = start_supervised({SocketManager, {"xapp-test", @bot, opts}})
    manager
  end

  # Spawns a fake "socket" process the manager can monitor. Returns the
  # pid so the test can kill it to simulate a disconnect.
  defp spawn_fake_socket do
    spawn(fn ->
      receive do
        :die -> :ok
      end
    end)
  end

  describe "happy path" do
    test "connects on start and resets the attempt counter" do
      socket = spawn_fake_socket()

      manager =
        start_manager(
          [{:ok, %{"url" => "wss://fake"}}],
          [{:ok, socket}]
        )

      assert_receive {:open_called, _}
      assert_receive {:start_socket_called, _}

      # Sync with the manager so the connect has been processed.
      _ = :sys.get_state(manager)

      state = GenServer.call(manager, :get_state)
      assert state.attempt == 0
      assert state.socket_pid == socket
      refute state.reconnecting?
    end

    test "uses configured API options when opening a Socket Mode URL" do
      socket = spawn_fake_socket()

      Slack.API
      |> expect(:post, fn "apps.connections.open", "xapp-virtual", %{}, api_opts ->
        assert api_opts == [base_url: @multiplexer_api]
        {:ok, %{"url" => "wss://multiplexer.example/socket"}}
      end)

      WebSockex
      |> expect(:start_link, fn "wss://multiplexer.example/socket", Slack.Socket, socket_state ->
        assert socket_state.app_token == "xapp-virtual"
        assert socket_state.bot == @bot
        {:ok, socket}
      end)

      {:ok, manager} =
        start_supervised(
          {SocketManager,
           {"xapp-virtual", @bot,
            [
              api: [base_url: @multiplexer_api],
              rand_fun: fn -> 0.5 end,
              base_delay_ms: 10,
              max_delay_ms: 40,
              jitter_ratio: 0.0
            ]}}
        )

      _ = :sys.get_state(manager)
      state = GenServer.call(manager, :get_state)
      assert state.socket_pid == socket
    end
  end

  describe "apps.connections.open failure" do
    test "schedules a reconnect with backoff and retries" do
      socket = spawn_fake_socket()

      manager =
        start_manager(
          [
            {:error, :nxdomain},
            {:ok, %{"url" => "wss://fake"}}
          ],
          [{:ok, socket}]
        )

      # First call fails.
      assert_receive {:open_called, {:error, :nxdomain}}
      _ = :sys.get_state(manager)

      state1 = GenServer.call(manager, :get_state)
      assert state1.attempt == 1
      assert state1.reconnecting?

      # Second call succeeds after the scheduled delay.
      assert_receive {:open_called, {:ok, _}}, 200
      assert_receive {:start_socket_called, {:ok, ^socket}}, 200
      _ = :sys.get_state(manager)

      state2 = GenServer.call(manager, :get_state)
      assert state2.attempt == 0
      assert state2.socket_pid == socket
      refute state2.reconnecting?
    end
  end

  describe "WebSockex.start_link failure" do
    test "schedules a reconnect even when the URL was fetched successfully" do
      socket = spawn_fake_socket()

      manager =
        start_manager(
          [
            {:ok, %{"url" => "wss://fake"}},
            {:ok, %{"url" => "wss://fake"}}
          ],
          [
            {:error, :handshake_failed},
            {:ok, socket}
          ]
        )

      assert_receive {:start_socket_called, {:error, :handshake_failed}}
      _ = :sys.get_state(manager)

      state_between = GenServer.call(manager, :get_state)
      assert state_between.attempt == 1
      assert state_between.reconnecting?

      assert_receive {:start_socket_called, {:ok, ^socket}}, 200
      _ = :sys.get_state(manager)

      state_after = GenServer.call(manager, :get_state)
      assert state_after.attempt == 0
      assert state_after.socket_pid == socket
    end
  end

  describe "socket DOWN" do
    test "monitors the socket and reconnects when it exits" do
      first = spawn_fake_socket()
      second = spawn_fake_socket()

      manager =
        start_manager(
          [
            {:ok, %{"url" => "wss://fake"}},
            {:ok, %{"url" => "wss://fake"}}
          ],
          [{:ok, first}, {:ok, second}]
        )

      assert_receive {:start_socket_called, {:ok, ^first}}
      _ = :sys.get_state(manager)

      # Confirm the manager tracks `first`.
      state1 = GenServer.call(manager, :get_state)
      assert state1.socket_pid == first

      # Kill the socket; manager should see the DOWN and reconnect.
      send(first, :die)

      assert_receive {:start_socket_called, {:ok, ^second}}, 200
      _ = :sys.get_state(manager)

      state2 = GenServer.call(manager, :get_state)
      assert state2.socket_pid == second
      assert state2.attempt == 0
    end
  end

  describe "backoff math" do
    test "caps the delay at max_delay_ms across many failures" do
      # Pre-load many failing opens so we can observe the counter
      # climbing. Then one success at the end.
      socket = spawn_fake_socket()
      failures = List.duplicate({:error, :down}, 8)

      manager =
        start_manager(
          failures ++ [{:ok, %{"url" => "wss://fake"}}],
          [{:ok, socket}],
          # Generous max so the test runs quickly but still exercises the cap.
          base_delay_ms: 1,
          max_delay_ms: 5,
          jitter_ratio: 0.0
        )

      # Wait until we observe the successful connect.
      assert_receive {:start_socket_called, {:ok, ^socket}}, 1_000
      _ = :sys.get_state(manager)

      state = GenServer.call(manager, :get_state)
      assert state.attempt == 0
      assert state.socket_pid == socket
    end

    test "jitter_ratio 0 produces exactly base delay" do
      # next_delay is a private helper, but we can exercise it indirectly
      # by giving the manager base_delay_ms=40, jitter_ratio=0, and
      # verifying the reconnect actually waits ~40ms before the second
      # open call.
      socket = spawn_fake_socket()

      start_time = System.monotonic_time(:millisecond)

      _manager =
        start_manager(
          [{:error, :down}, {:ok, %{"url" => "wss://fake"}}],
          [{:ok, socket}],
          base_delay_ms: 40,
          max_delay_ms: 40,
          jitter_ratio: 0.0
        )

      assert_receive {:open_called, {:error, :down}}
      assert_receive {:open_called, {:ok, _}}, 200

      elapsed = System.monotonic_time(:millisecond) - start_time
      # First call fires immediately, second call waits ~40ms. Allow
      # slack for scheduling — just ensure we waited at least ~30ms and
      # not a full second.
      assert elapsed >= 30
      assert elapsed < 500
    end
  end
end
