defmodule KafkaEx.Client.PipeliningTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Network.Socket

  @node_id 1

  setup do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: true])
    {:ok, server} = :gen_tcp.accept(listen, 2_000)

    on_exit(fn ->
      :gen_tcp.close(server)
      :gen_tcp.close(client)
      :gen_tcp.close(listen)
    end)

    %{client: client, server: server, port: port, state: state_for(client, port)}
  end

  defp state_for(socket, port) do
    broker = %Broker{
      node_id: @node_id,
      host: "127.0.0.1",
      port: port,
      socket: %Socket{socket: socket, ssl: false}
    }

    %State{
      cluster_metadata: %ClusterMetadata{brokers: %{@node_id => broker}},
      api_versions: %{},
      correlation_id: 100
    }
  end

  defp request, do: %Kayrock.ApiVersions.V0.Request{}

  defp selector, do: NodeSelector.node_id(@node_id)

  # ApiVersions v0: correlation id, error code, then an empty api-key array.
  defp response_frame(correlation_id) do
    <<correlation_id::32-signed, 0::16-signed, 0::32-signed>>
  end

  defp send_async(state) do
    {:reply, reply, state} =
      Client.handle_call({:kayrock_request_async, request(), selector()}, {self(), make_ref()}, state)

    {reply, state}
  end

  # The correlation id a request went out with, read off the wire.
  defp correlation_id_of(<<_api_key::16, _api_vsn::16, correlation_id::32-signed, _rest::binary>>) do
    correlation_id
  end

  defp await_request(server) do
    {:ok, wire} = :gen_tcp.recv(server, 0, 2_000)
    correlation_id_of(wire)
  end

  # Hands the fake broker's socket to a process that answers `frames` only once
  # it has read one more request: the responses then stay in the socket buffer
  # instead of racing the client's flip to a passive read.
  defp answer_after_next_request(server, frames) do
    test = self()

    responder =
      spawn_link(fn ->
        receive do: (:go -> :ok)
        {:ok, _wire} = :gen_tcp.recv(server, 0, 2_000)
        Enum.each(frames, &:gen_tcp.send(server, &1))
        send(test, :answered)
        receive do: (:stop -> :ok)
      end)

    :ok = :gen_tcp.controlling_process(server, responder)
    send(responder, :go)
    responder
  end

  test "pipelined responses reach their own requests in connection order", ctx do
    {{:ok, ref_a}, state} = send_async(ctx.state)
    {{:ok, ref_b}, state} = send_async(state)

    corr_a = await_request(ctx.server)
    corr_b = await_request(ctx.server)
    assert corr_b == corr_a + 1

    :ok = :gen_tcp.send(ctx.server, response_frame(corr_a))
    :ok = :gen_tcp.send(ctx.server, response_frame(corr_b))

    client = ctx.client
    assert_receive {:tcp, ^client, frame_a}, 2_000
    {:noreply, state} = Client.handle_info({:tcp, client, frame_a}, state)
    assert_receive {:kafka_ex_response, ^ref_a, {:ok, %Kayrock.ApiVersions.V0.Response{correlation_id: ^corr_a}}}

    assert_receive {:tcp, ^client, frame_b}, 2_000
    {:noreply, state} = Client.handle_info({:tcp, client, frame_b}, state)
    assert_receive {:kafka_ex_response, ^ref_b, {:ok, %Kayrock.ApiVersions.V0.Response{correlation_id: ^corr_b}}}

    refute State.pending?(state, client)
  end

  test "a frame carrying the wrong correlation id closes the connection and fails everything on it", ctx do
    {{:ok, ref_a}, state} = send_async(ctx.state)
    {{:ok, ref_b}, state} = send_async(state)

    corr_a = await_request(ctx.server)
    _corr_b = await_request(ctx.server)

    {:noreply, state} = Client.handle_info({:tcp, ctx.client, response_frame(corr_a + 7)}, state)

    assert_receive {:kafka_ex_response, ^ref_a, {:error, :correlation_mismatch}}
    assert_receive {:kafka_ex_response, ^ref_b, {:error, :correlation_mismatch}}
    refute State.pending?(state, ctx.client)
    refute Broker.connected?(broker_of(state))
  end

  test "a closed connection fails every request outstanding on it", ctx do
    {{:ok, ref_a}, state} = send_async(ctx.state)
    {{:ok, ref_b}, state} = send_async(state)
    _ = await_request(ctx.server)
    _ = await_request(ctx.server)

    {:noreply, state} = Client.handle_info({:tcp_closed, ctx.client}, state)

    assert_receive {:kafka_ex_response, ^ref_a, {:error, :closed}}
    assert_receive {:kafka_ex_response, ^ref_b, {:error, :closed}}
    refute State.pending?(state, ctx.client)
  end

  test "a synchronous request behind a pipelined one is answered in its turn", ctx do
    {{:ok, ref_a}, state} = send_async(ctx.state)
    corr_a = await_request(ctx.server)
    corr_sync = corr_a + 1

    responder = answer_after_next_request(ctx.server, [response_frame(corr_a), response_frame(corr_sync)])

    {:reply, reply, state} =
      Client.handle_call({:kayrock_request, request(), selector()}, {self(), make_ref()}, state)

    assert {:ok, %Kayrock.ApiVersions.V0.Response{correlation_id: ^corr_sync}} = reply
    assert_received :answered

    # The pipelined response read past on the way to ours comes back through the
    # client's own mailbox, so it still reaches the request that asked for it.
    client = ctx.client
    assert_receive {:tcp, ^client, frame_a}, 2_000
    {:noreply, state} = Client.handle_info({:tcp, client, frame_a}, state)
    assert_receive {:kafka_ex_response, ^ref_a, {:ok, %Kayrock.ApiVersions.V0.Response{correlation_id: ^corr_a}}}

    refute State.pending?(state, client)
    send(responder, :stop)
  end

  test "a synchronous request with nothing pending keeps the passive path and leaves the socket active", ctx do
    responder = answer_after_next_request(ctx.server, [response_frame(100)])

    {:reply, reply, state} =
      Client.handle_call({:kayrock_request, request(), selector()}, {self(), make_ref()}, ctx.state)

    assert {:ok, %Kayrock.ApiVersions.V0.Response{correlation_id: 100}} = reply
    assert {:ok, [active: true]} = :inet.getopts(ctx.client, [:active])
    refute State.pending?(state, ctx.client)
    send(responder, :stop)
  end

  defp broker_of(state), do: state |> State.brokers() |> List.first()
end
