defmodule Slack.API do
  @moduledoc """
  Slack Web API.
  """

  require Logger

  @base_url "https://slack.com/api"
  @max_retries 3

  @doc """
  Req client for Slack API.

  Includes automatic retry with jittered backoff for transient errors (429, 5xx).
  Respects Slack's `Retry-After` header on 429 responses, adding jitter to
  avoid thundering-herd problems.
  """
  @spec client(String.t(), keyword()) :: Req.Request.t()
  def client(token, opts \\ []) do
    Req.new(
      base_url: base_url(opts),
      auth: {:bearer, token},
      retry: &retry_with_retry_after/2,
      max_retries: @max_retries
    )
  end

  @doc """
  GET from Slack API.
  """
  @spec get(String.t(), String.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def get(endpoint, token, args \\ %{}, opts \\ []) do
    result =
      Req.get(client(token, opts),
        url: endpoint,
        params: args
      )

    case result do
      {:ok, %{body: %{"ok" => true} = body}} ->
        {:ok, body}

      {_, error} ->
        Logger.error(inspect(error))
        {:error, error}
    end
  end

  @doc """
  POST to Slack API.
  """
  @spec post(String.t(), String.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def post(endpoint, token, args \\ %{}, opts \\ []) do
    result =
      Req.post(client(token, opts),
        url: endpoint,
        form: args
      )

    case result do
      {:ok, %{body: %{"ok" => true} = body}} ->
        {:ok, body}

      {_, error} ->
        Logger.error(inspect(error))
        {:error, error}
    end
  end

  @doc """
  GET pages from Slack API as a `Stream`.

  Paginates using cursor-based pagination. The stream halts when:
  - The cursor is empty (`""`) or `nil`
  - A duplicate cursor is detected (Slack returned the same cursor twice)
  - An empty page is returned after data has been fetched
  - An API error occurs (returns partial data collected so far instead of raising)

  You can start at a cursor if you pass in `:cursor` as one of the `args`.
  """
  @spec stream(String.t(), String.t(), String.t(), map() | keyword(), keyword()) ::
          Enumerable.t()
  def stream(endpoint, token, resource, args \\ %{}, opts \\ []) do
    {starting_cursor, args} =
      args
      |> Map.new()
      |> Map.pop(:cursor, nil)

    # Accumulator: {current_cursor, previous_cursor, pages_fetched}
    Stream.resource(
      fn -> {starting_cursor, nil, 0} end,
      fn
        {"", _, _} ->
          {:halt, nil}

        {cursor, prev_cursor, _page} when cursor == prev_cursor and cursor != nil ->
          Logger.warning("[Slack.API] Duplicate cursor detected, stopping pagination")
          {:halt, nil}

        {cursor, _prev_cursor, page_count} ->
          params = if cursor, do: Map.put(args, :cursor, cursor), else: args

          case get_page(endpoint, token, params, opts) do
            {:ok, %{^resource => data} = body} ->
              next = get_in(body, ["response_metadata", "next_cursor"]) || ""

              case data do
                [] when page_count > 0 ->
                  Logger.warning("[Slack.API] Empty page with cursor, stopping pagination")

                  {:halt, nil}

                _ ->
                  {data, {next, cursor, page_count + 1}}
              end

            {:error, reason} ->
              Logger.error("[Slack.API] Pagination error: #{inspect(reason)}")
              {:halt, nil}
          end
      end,
      fn _ -> :ok end
    )
  end

  # Custom retry function for Req (2-arity: receives request + response/exception).
  #
  # On 429 responses: parses the Retry-After header and returns {:delay, ms} with
  # added jitter. Falls back to jittered exponential backoff when the header is absent.
  #
  # On other transient errors (408, 5xx, transport errors): uses jittered exponential
  # backoff via {:delay, ms}.
  @doc false
  def retry_with_retry_after(request, response_or_exception) do
    retry_count = Req.Request.get_private(request, :req_retry_count, 0)

    case response_or_exception do
      %Req.Response{status: 429} = response ->
        delay =
          case Req.Response.get_retry_after(response) do
            ms when is_integer(ms) and ms > 0 -> ms + jitter()
            _ -> jittered_backoff(retry_count)
          end

        {:delay, delay}

      %Req.Response{status: status} when status in [408, 500, 502, 503, 504] ->
        {:delay, jittered_backoff(retry_count)}

      %Req.TransportError{reason: reason}
      when reason in [:timeout, :econnrefused, :closed] ->
        {:delay, jittered_backoff(retry_count)}

      _ ->
        false
    end
  end

  # Exponential backoff with jitter: base * 2^n + random(0..1000)ms
  defp jittered_backoff(retry_count) do
    base = Integer.pow(2, retry_count) * 1_000
    base + jitter()
  end

  defp jitter, do: :rand.uniform(1_000)

  defp get_page(endpoint, token, params, []), do: __MODULE__.get(endpoint, token, params)
  defp get_page(endpoint, token, params, opts), do: __MODULE__.get(endpoint, token, params, opts)

  defp base_url(opts) do
    opts
    |> Keyword.get(:base_url, @base_url)
    |> String.trim_trailing("/")
  end
end
