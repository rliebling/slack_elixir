defmodule Slack.ChannelServerTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Slack.TestBot

  @bot %Slack.Bot{
    id: "bot-123-ABC",
    module: TestBot,
    token: "xoxb-test-token",
    team_id: "team-123-ABC",
    user_id: "user-123-ABC"
  }

  @multiplexer_api "http://localhost:4000/api"

  setup :set_mimic_global

  setup do
    start_supervised!({Registry, keys: :unique, name: Slack.ChannelServerRegistry})
    start_supervised!({Registry, keys: :unique, name: Slack.MessageServerRegistry})
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: Slack.DynamicSupervisor})

    stub(Slack.MessageServer, :start_supervised, fn _bot, _channel ->
      {:ok, spawn(fn -> Process.sleep(:infinity) end)}
    end)

    :ok
  end

  describe "start_link/1 and channel fetching" do
    test "fetches channels asynchronously on startup" do
      test_pid = self()

      Slack.API
      |> expect(:stream, fn "users.conversations", _token, "channels", opts ->
        assert Keyword.get(opts, :types) == "public_channel"
        assert Keyword.get(opts, :exclude_archived) == true

        [%{"id" => "C1"}, %{"id" => "C2"}]
      end)

      Slack.MessageServer
      |> expect(:start_supervised, 2, fn _bot, channel ->
        send(test_pid, {:joined, channel})
        {:ok, spawn(fn -> Process.sleep(:infinity) end)}
      end)

      {:ok, pid} = Slack.ChannelServer.start_link({@bot, []})

      # Wait for channels to be joined
      assert_receive {:joined, _}, 1_000
      assert_receive {:joined, _}, 1_000
      Process.sleep(50)

      assert Process.alive?(pid)
      state = :sys.get_state(pid)
      assert Enum.sort(state.channels) == ["C1", "C2"]
    end

    test "starts with empty channels when stream returns nothing" do
      Slack.API
      |> expect(:stream, fn "users.conversations", _token, "channels", _opts ->
        # stream/4 returns empty list on API errors (halts instead of raising)
        []
      end)

      {:ok, pid} = Slack.ChannelServer.start_link({@bot, []})

      # Give handle_continue time to execute
      Process.sleep(100)

      assert Process.alive?(pid)
      state = :sys.get_state(pid)
      assert state.channels == []
    end

    test "passes configured API options to channel listing" do
      bot = %{@bot | api_opts: [base_url: @multiplexer_api]}

      Slack.API
      |> expect(:stream, fn "users.conversations",
                            "xoxb-test-token",
                            "channels",
                            opts,
                            api_opts ->
        assert Keyword.get(opts, :types) == "public_channel"
        assert Keyword.get(opts, :exclude_archived) == true
        assert api_opts == [base_url: @multiplexer_api]
        []
      end)

      {:ok, _pid} = Slack.ChannelServer.start_link({bot, []})
      Process.sleep(100)
    end
  end

  describe "default channel types" do
    test "defaults to public_channel only" do
      Slack.API
      |> expect(:stream, fn "users.conversations", _token, "channels", opts ->
        assert Keyword.get(opts, :types) == "public_channel"
        []
      end)

      {:ok, _pid} = Slack.ChannelServer.start_link({@bot, []})
      Process.sleep(100)
    end

    test "accepts custom types as string" do
      Slack.API
      |> expect(:stream, fn "users.conversations", _token, "channels", opts ->
        assert Keyword.get(opts, :types) == "public_channel,private_channel"
        []
      end)

      {:ok, _pid} =
        Slack.ChannelServer.start_link({@bot, [types: "public_channel,private_channel"]})

      Process.sleep(100)
    end

    test "accepts custom types as list" do
      Slack.API
      |> expect(:stream, fn "users.conversations", _token, "channels", opts ->
        assert Keyword.get(opts, :types) == "public_channel,mpim"
        []
      end)

      {:ok, _pid} =
        Slack.ChannelServer.start_link({@bot, [types: ["public_channel", "mpim"]]})

      Process.sleep(100)
    end
  end

  describe "exclude_archived" do
    test "sends exclude_archived: true by default" do
      Slack.API
      |> expect(:stream, fn "users.conversations", _token, "channels", opts ->
        assert Keyword.get(opts, :exclude_archived) == true
        []
      end)

      {:ok, _pid} = Slack.ChannelServer.start_link({@bot, []})
      Process.sleep(100)
    end
  end

  describe "join and part" do
    test "join starts a MessageServer for the channel" do
      Slack.API
      |> expect(:stream, fn "users.conversations", _token, "channels", _opts ->
        []
      end)

      # First call: from the join cast
      Slack.MessageServer
      |> expect(:start_supervised, fn bot, channel ->
        assert bot == @bot
        assert channel == "C_NEW"
        {:ok, spawn(fn -> Process.sleep(:infinity) end)}
      end)

      {:ok, _pid} = Slack.ChannelServer.start_link({@bot, []})
      Process.sleep(100)

      Slack.ChannelServer.join(@bot, "C_NEW")
      Process.sleep(100)
    end
  end
end
