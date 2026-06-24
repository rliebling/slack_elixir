defmodule Slack.APITest do
  use ExUnit.Case, async: false
  use Mimic

  @token "xoxb-test-token"

  setup :set_mimic_global

  describe "stream/4 pagination" do
    test "returns empty list for empty workspace (first page empty with no cursor)" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:ok,
         %{
           "channels" => [],
           "response_metadata" => %{"next_cursor" => ""}
         }}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      assert result == []
    end

    test "halts on empty cursor" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C1"}],
           "response_metadata" => %{"next_cursor" => ""}
         }}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      assert result == [%{"id" => "C1"}]
    end

    test "halts on nil cursor (missing response_metadata)" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:ok, %{"channels" => [%{"id" => "C1"}]}}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      assert result == [%{"id" => "C1"}]
    end

    test "follows cursors across multiple pages" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C1"}],
           "response_metadata" => %{"next_cursor" => "cursor_page2"}
         }}
      end)
      |> expect(:get, fn "test.list",
                         @token,
                         %{types: "public_channel", cursor: "cursor_page2"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C2"}],
           "response_metadata" => %{"next_cursor" => ""}
         }}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      assert result == [%{"id" => "C1"}, %{"id" => "C2"}]
    end

    test "halts on duplicate cursor" do
      # First call returns data with a cursor
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C1"}],
           "response_metadata" => %{"next_cursor" => "stuck_cursor"}
         }}
      end)
      # Second call returns the same cursor — infinite loop detected
      |> expect(:get, fn "test.list",
                         @token,
                         %{types: "public_channel", cursor: "stuck_cursor"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C2"}],
           "response_metadata" => %{"next_cursor" => "stuck_cursor"}
         }}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      # Should get data from both pages but halt when cursor repeats
      assert result == [%{"id" => "C1"}, %{"id" => "C2"}]
    end

    test "halts on empty batch after data was fetched" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C1"}],
           "response_metadata" => %{"next_cursor" => "cursor_page2"}
         }}
      end)
      |> expect(:get, fn "test.list",
                         @token,
                         %{types: "public_channel", cursor: "cursor_page2"} ->
        {:ok,
         %{
           "channels" => [],
           "response_metadata" => %{"next_cursor" => "cursor_page3"}
         }}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      # Gets page 1 data, halts on empty page 2
      assert result == [%{"id" => "C1"}]
    end

    test "returns partial data on API error instead of raising" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C1"}],
           "response_metadata" => %{"next_cursor" => "cursor_page2"}
         }}
      end)
      |> expect(:get, fn "test.list",
                         @token,
                         %{types: "public_channel", cursor: "cursor_page2"} ->
        {:error, %Req.Response{status: 500, body: "Internal Server Error"}}
      end)

      # Should NOT raise — returns data collected so far
      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      assert result == [%{"id" => "C1"}]
    end

    test "returns empty list on immediate API error" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{types: "public_channel"} ->
        {:error, %Req.Response{status: 403, body: %{"ok" => false, "error" => "not_authed"}}}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", types: "public_channel")
        |> Enum.to_list()

      assert result == []
    end

    test "sends cursor param (not next_cursor) to Slack API" do
      # This test verifies the fix: the API expects `cursor`, not `next_cursor`
      Slack.API
      |> expect(:get, fn "test.list", @token, params ->
        # First call should NOT have a cursor param
        refute Map.has_key?(params, :cursor)
        refute Map.has_key?(params, :next_cursor)

        {:ok,
         %{
           "channels" => [%{"id" => "C1"}],
           "response_metadata" => %{"next_cursor" => "abc123"}
         }}
      end)
      |> expect(:get, fn "test.list", @token, params ->
        # Second call must use `cursor` (not `next_cursor`)
        assert params[:cursor] == "abc123"
        refute Map.has_key?(params, :next_cursor)

        {:ok,
         %{
           "channels" => [%{"id" => "C2"}],
           "response_metadata" => %{"next_cursor" => ""}
         }}
      end)

      "test.list"
      |> Slack.API.stream(@token, "channels", types: "public_channel")
      |> Enum.to_list()
    end

    test "accepts starting cursor via :cursor arg" do
      Slack.API
      |> expect(:get, fn "test.list", @token, %{cursor: "start_here"} ->
        {:ok,
         %{
           "channels" => [%{"id" => "C5"}],
           "response_metadata" => %{"next_cursor" => ""}
         }}
      end)

      result =
        "test.list"
        |> Slack.API.stream(@token, "channels", cursor: "start_here")
        |> Enum.to_list()

      assert result == [%{"id" => "C5"}]
    end
  end

  describe "client/1" do
    test "configures custom retry function" do
      client = Slack.API.client(@token)
      assert is_function(client.options.retry, 2)
    end

    test "configures max_retries" do
      client = Slack.API.client(@token)
      assert client.options.max_retries == 3
    end

    test "does not set retry_delay (handled by custom retry function)" do
      client = Slack.API.client(@token)
      refute Map.has_key?(client.options, :retry_delay)
    end

    test "defaults to Slack's Web API base URL" do
      client = Slack.API.client(@token)
      assert client.options.base_url == "https://slack.com/api"
    end

    test "accepts an alternate Slack-compatible Web API base URL" do
      client = Slack.API.client(@token, base_url: "http://localhost:4000/api/")
      assert client.options.base_url == "http://localhost:4000/api"
    end
  end

  describe "retry_with_retry_after/2" do
    test "respects Retry-After header on 429 with jitter" do
      request = %Req.Request{options: %{}}

      response = %Req.Response{
        status: 429,
        headers: %{"retry-after" => ["30"]}
      }

      assert {:delay, delay} = Slack.API.retry_with_retry_after(request, response)
      # 30s = 30_000ms + jitter (1..1000ms)
      assert delay >= 30_001 and delay <= 31_000
    end

    test "falls back to jittered backoff on 429 without Retry-After" do
      request = Req.Request.put_private(%Req.Request{options: %{}}, :req_retry_count, 0)

      response = %Req.Response{
        status: 429,
        headers: %{}
      }

      assert {:delay, delay} = Slack.API.retry_with_retry_after(request, response)
      # Retry 0: base 1000 + jitter(1..1000)
      assert delay >= 1_001 and delay <= 2_000
    end

    test "retries on 500 with jittered backoff" do
      request = Req.Request.put_private(%Req.Request{options: %{}}, :req_retry_count, 1)

      response = %Req.Response{status: 500, headers: [], body: ""}

      assert {:delay, delay} = Slack.API.retry_with_retry_after(request, response)
      # Retry 1: base 2000 + jitter(1..1000)
      assert delay >= 2_001 and delay <= 3_000
    end

    test "retries on transport errors" do
      request = Req.Request.put_private(%Req.Request{options: %{}}, :req_retry_count, 0)

      error = %Req.TransportError{reason: :timeout}

      assert {:delay, delay} = Slack.API.retry_with_retry_after(request, error)
      assert delay >= 1_001 and delay <= 2_000
    end

    test "does not retry on non-transient responses" do
      request = %Req.Request{options: %{}}
      response = %Req.Response{status: 401, headers: [], body: ""}

      assert Slack.API.retry_with_retry_after(request, response) == false
    end

    test "does not retry on non-transient exceptions" do
      request = %Req.Request{options: %{}}
      error = %RuntimeError{message: "boom"}

      assert Slack.API.retry_with_retry_after(request, error) == false
    end
  end
end
