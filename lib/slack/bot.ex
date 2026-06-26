defmodule Slack.Bot do
  @moduledoc """
  The Slack Bot.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          module: module(),
          api_opts: keyword(),
          team_id: String.t(),
          token: String.t(),
          user_id: String.t()
        }

  @derive {Inspect, except: [:token]}
  @enforce_keys [:id, :module, :token, :team_id, :user_id]
  defstruct [:id, :module, :token, :team_id, :user_id, api_opts: []]

  @doc """
  Handle the event from Slack.
  Return value is ignored.
  """
  @callback handle_event(type :: String.t(), payload :: map(), t()) :: any()

  @doc """
  Handle the full Socket Mode envelope from Slack.

  Implement this optional callback when the caller needs to inspect the full
  envelope or control the upstream acknowledgment. Return `{:ack, map}` to send
  a custom acknowledgment body. If this callback is not implemented, the socket
  keeps the existing `handle_event/3` behavior and sends a minimal envelope ack.
  """
  @callback handle_envelope(envelope :: map(), t()) :: :ok | {:ack, map()} | any()

  @optional_callbacks handle_envelope: 2

  defmacro __using__(_opts) do
    quote do
      import Slack.Bot
      @behaviour Slack.Bot
    end
  end

  # Build a Bot struct from a string-keyed map.
  @doc false
  def from_string_params(bot_module, bot_token, params, api_opts \\ []) do
    %__MODULE__{
      id: Map.fetch!(params, "bot_id"),
      module: bot_module,
      api_opts: api_opts,
      team_id: Map.fetch!(params, "team_id"),
      token: bot_token,
      user_id: Map.fetch!(params, "user_id")
    }
  end

  @doc """
  Send a message to a channel.

  The `message` can be just the message text, or a `t:map/0` of properties that
  are accepted by Slack's `chat.postMessage` API endpoint.
  """
  @spec send_message(String.t(), String.t() | map()) :: Macro.t()
  defmacro send_message(channel, message) do
    quote do
      Slack.MessageServer.send(__MODULE__, unquote(channel), unquote(message))
    end
  end
end
