defmodule KafkaEx.Client.RequestTimeoutTest do
  # Sets the global `:sync_timeout`, so not async.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Network.Socket

  @node_id 1
  @group "request-timeout-group"
  @sync_timeout 100
  # Longer than `@sync_timeout`, well inside each request's own timeout.
  @broker_delay 300

  setup do
    previous = Application.fetch_env(:kafka_ex, :sync_timeout)
    Application.put_env(:kafka_ex, :sync_timeout, @sync_timeout)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:kafka_ex, :sync_timeout, value)
        :error -> Application.delete_env(:kafka_ex, :sync_timeout)
      end
    end)

    %{listen: listen, socket: client, server: server, port: port} = KafkaEx.TestHelpers.broker_socket_pair()

    %{listen: listen, server: server, state: state_for(client, port)}
  end

  defp state_for(socket, port) do
    broker = %Broker{node_id: @node_id, host: "127.0.0.1", port: port, socket: %Socket{socket: socket, ssl: false}}

    %State{
      cluster_metadata: %ClusterMetadata{
        brokers: %{@node_id => broker},
        consumer_group_coordinators: %{@group => @node_id}
      },
      api_versions: %{},
      correlation_id: 100
    }
  end

  defp join_opts(timeout), do: [api_version: 0, topics: ["t"], session_timeout: 10_000, timeout: timeout]

  # JoinGroup v0: error code, generation, protocol, leader, member, no members.
  defp join_response_frame(correlation_id) do
    <<correlation_id::32-signed, 0::16-signed, 1::32-signed, 5::16, "range", 2::16, "m1", 2::16, "m1", 0::32-signed>>
  end

  # ApiVersions v0: error code, then an empty api-key array.
  defp api_versions_frame(correlation_id), do: <<correlation_id::32-signed, 0::16-signed, 0::32-signed>>

  # A broker that holds the request it reads for `@broker_delay` before answering.
  defp answer_late(server, frame_fn) do
    responder =
      spawn_link(fn ->
        receive do: (:go -> :ok)
        {:ok, <<_key::16, _vsn::16, correlation_id::32-signed, _::binary>>} = :gen_tcp.recv(server, 0, 2_000)
        Process.sleep(@broker_delay)
        :ok = :gen_tcp.send(server, frame_fn.(correlation_id))
        receive do: (:stop -> :ok)
      end)

    :ok = :gen_tcp.controlling_process(server, responder)
    send(responder, :go)
    responder
  end

  test "a join waits for the broker as long as its own timeout, past the default", ctx do
    responder = answer_late(ctx.server, &join_response_frame/1)

    log =
      capture_log(fn ->
        assert {:reply, {:ok, %JoinGroup{generation_id: 1, leader_id: "m1"}}, _state} =
                 Client.handle_call({:join_group, @group, "", join_opts(5_000)}, {self(), make_ref()}, ctx.state)
      end)

    refute log =~ "timed out"
    send(responder, :stop)
  end

  test "a join is not retried once its own timeout is spent", ctx do
    log =
      capture_log(fn ->
        assert {:reply, {:error, _}, _state} =
                 Client.handle_call({:join_group, @group, "", join_opts(200)}, {self(), make_ref()}, ctx.state)
      end)

    assert [_one] = Regex.scan(~r/Receiving data from broker "127.0.0.1":\d+ timed out/, log)
    # A retry would have dialled the broker again.
    assert {:error, :timeout} = :gen_tcp.accept(ctx.listen, 0)
  end

  test "a raw request waits as long as the timeout it names", ctx do
    responder = answer_late(ctx.server, &api_versions_frame/1)
    request = %Kayrock.ApiVersions.V0.Request{}

    assert {:reply, {:ok, %Kayrock.ApiVersions.V0.Response{}}, _state} =
             Client.handle_call(
               {:kayrock_request, request, NodeSelector.node_id(@node_id), 5_000},
               {self(), make_ref()},
               ctx.state
             )

    send(responder, :stop)
  end

  # Stands in for the client: answers the call with the options it carried.
  defp echo_client(tag) do
    spawn_link(fn ->
      receive do
        {:"$gen_call", from, call} when elem(call, 0) == tag ->
          GenServer.reply(from, {:error, {:opts, elem(call, tuple_size(call) - 1)}})
      end
    end)
  end

  test "join_group and sync_group hand their timeout to the client" do
    assert {:error, {:opts, opts}} = API.join_group(echo_client(:join_group), @group, "", timeout: 7_000)
    assert opts[:timeout] == 7_000

    assert {:error, {:opts, opts}} = API.join_group(echo_client(:join_group), @group, "", rebalance_timeout: 4_000)
    assert opts[:timeout] == 14_000

    assert {:error, {:opts, opts}} = API.sync_group(echo_client(:sync_group), @group, 1, "m1")
    assert opts[:timeout] == 35_000
  end
end
