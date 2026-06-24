defmodule Slack.Supervisor do
  @moduledoc """
  Supervisor that starts the stuff that needs to run.
  """
  use Supervisor

  require Logger

  @doc """
  Start the Slack bot supervisor.
  See `README` for instructions.
  """
  @spec start_link(config :: keyword()) :: Supervisor.on_start()
  def start_link(bot_config) do
    Supervisor.start_link(__MODULE__, bot_config)
  end

  @impl true
  def init(bot_config) do
    {app_token, bot_config} = Keyword.pop!(bot_config, :app_token)
    {bot_token, bot_config} = Keyword.pop!(bot_config, :bot_token)
    {bot_module, bot_config} = Keyword.pop!(bot_config, :bot)
    {api_opts, bot_config} = Keyword.pop(bot_config, :api, [])
    {channel_config, bot_config} = Keyword.pop(bot_config, :channels, [])
    {socket_opts, _bot_config} = Keyword.pop(bot_config, :socket, [])

    bot = fetch_identity!(bot_token, bot_module, api_opts)
    socket_opts = Keyword.put_new(socket_opts, :api, api_opts)

    children = [
      {Registry, keys: :unique, name: Slack.ChannelServerRegistry},
      {Registry, keys: :unique, name: Slack.MessageServerRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Slack.DynamicSupervisor},
      {PartitionSupervisor, child_spec: Task.Supervisor, name: Slack.TaskSupervisors},
      {Slack.ChannelServer, {bot, channel_config}},
      {Slack.SocketManager, {app_token, bot, socket_opts}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp fetch_identity!(bot_token, bot_module, api_opts) do
    case api_get("auth.test", bot_token, %{}, api_opts) do
      {:ok, %{"ok" => true, "bot_id" => _} = body} ->
        Slack.Bot.from_string_params(bot_module, bot_token, body, api_opts)

      {_, result} ->
        Logger.error("[Slack.Supervisor] Error fetching user ID: #{inspect(result)}")
        raise "Unable to fetch bot user ID"
    end
  end

  defp api_get(endpoint, token, args, []), do: Slack.API.get(endpoint, token, args)
  defp api_get(endpoint, token, args, opts), do: Slack.API.get(endpoint, token, args, opts)
end
