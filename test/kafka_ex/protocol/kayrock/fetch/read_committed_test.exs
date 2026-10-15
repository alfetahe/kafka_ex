defmodule KafkaEx.Protocol.Kayrock.Fetch.ReadCommittedTest do
  use ExUnit.Case, async: true
  import Bitwise

  alias KafkaEx.Client
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Network.Socket
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  @transactional 0x10
  @control 0x20
  @abort 0
  @commit 1

  defp batch(attributes, producer_id, records),
    do: %RecordBatch{attributes: attributes, producer_id: producer_id, producer_epoch: 0, records: records}

  defp txn(producer_id, offset_values),
    do: batch(@transactional, producer_id, Enum.map(offset_values, fn {o, v} -> %Record{offset: o, value: v} end))

  defp marker(producer_id, type, offset),
    do:
      batch(@transactional ||| @control, producer_id, [
        %Record{offset: offset, key: <<0::16, type::16>>, value: <<0::16, 0::32>>}
      ])

  # The record set as it travels on the wire: a size prefix, then the batches.
  defp record_set_bytes(batches) do
    bytes = Enum.map_join(batches, fn b -> b |> RecordBatch.serialize() |> IO.iodata_to_binary() |> strip_size() end)
    <<byte_size(bytes)::32-signed, bytes::binary>>
  end

  defp strip_size(<<_size::32, bytes::binary>>), do: bytes

  defp response(batches, aborted) do
    <<size::32-signed, bytes::binary-size(size)>> = record_set_bytes(batches)

    %Kayrock.Fetch.V4.Response{
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "t",
          partition_responses: [
            %{
              partition_header: %{
                partition: 0,
                error_code: 0,
                high_watermark: 10,
                last_stable_offset: 10,
                aborted_transactions: aborted
              },
              record_set: RecordBatch.deserialize(bytes)
            }
          ]
        }
      ]
    }
  end

  defp values(fetch), do: Enum.map(fetch.records, & &1.value)

  # Producer 42 writes two records in a transaction and aborts it (an ABORT
  # marker at offset 2); producer 7 then commits one record at 3. A
  # read_committed fetch (Fetch v4+, isolation_level 1) answers with all three
  # batches and names producer 42's transaction from offset 0 as aborted.
  defp aborted_then_committed do
    response(
      [txn(42, [{0, "aborted-1"}, {1, "aborted-2"}]), marker(42, @abort, 2), txn(7, [{3, "committed"}])],
      [%{producer_id: 42, first_offset: 0}]
    )
  end

  test "a read_committed fetch hands over neither aborted records nor control markers" do
    {:ok, fetch} = ResponseParser.fetch_response(aborted_then_committed(), 1)

    assert values(fetch) == ["committed"]
    # The consumer resumes past everything the fetch covered, filtered or not.
    assert fetch.last_offset == 3
  end

  test "a read_committed fetch drops a commit marker when no transaction was aborted" do
    {:ok, fetch} = ResponseParser.fetch_response(response([txn(7, [{0, "committed"}]), marker(7, @commit, 1)], []), 1)

    assert values(fetch) == ["committed"]
    assert fetch.last_offset == 1
  end

  test "a producer's records after its abort marker are committed again" do
    batches = [txn(42, [{0, "aborted"}]), marker(42, @abort, 1), txn(42, [{2, "committed"}]), marker(42, @commit, 3)]
    {:ok, fetch} = ResponseParser.fetch_response(response(batches, [%{producer_id: 42, first_offset: 0}]), 1)

    assert values(fetch) == ["committed"]
    assert fetch.last_offset == 3
  end

  test "a read_committed fetch holding only an aborted transaction still covers its offsets" do
    batches = [txn(42, [{0, "aborted-1"}, {1, "aborted-2"}]), marker(42, @abort, 2)]
    {:ok, fetch} = ResponseParser.fetch_response(response(batches, [%{producer_id: 42, first_offset: 0}]), 1)

    assert fetch.records == []
    assert fetch.last_offset == 2
  end

  test "a read_uncommitted fetch hands over every record, markers included" do
    expected = ["aborted-1", "aborted-2", <<0::16, 0::32>>, "committed"]

    for parsed <- [
          ResponseParser.fetch_response(aborted_then_committed()),
          ResponseParser.fetch_response(aborted_then_committed(), 0)
        ] do
      assert {:ok, fetch} = parsed
      assert values(fetch) == expected
      assert fetch.last_offset == 3
    end
  end

  describe "through the client" do
    setup do
      %{socket: socket, server: server, port: port} = KafkaEx.TestHelpers.broker_socket_pair()
      broker = %Broker{node_id: 1, host: "127.0.0.1", port: port, socket: %Socket{socket: socket, ssl: false}}

      state = %State{
        cluster_metadata: %ClusterMetadata{
          brokers: %{1 => broker},
          topics: %{"t" => %Topic{name: "t", partition_leaders: %{0 => 1}}}
        },
        api_versions: %{1 => {0, 4}},
        correlation_id: 100
      }

      %{server: server, state: state}
    end

    # Fetch v4 answer for topic "t" partition 0: producer 42's aborted
    # transaction and its abort marker, nothing else.
    defp answer_all_aborted(server) do
      responder =
        spawn_link(fn ->
          receive do: (:go -> :ok)
          {:ok, <<1::16, 4::16, correlation_id::32-signed, _::binary>>} = :gen_tcp.recv(server, 0, 2_000)
          record_set = record_set_bytes([txn(42, [{0, "aborted-1"}, {1, "aborted-2"}]), marker(42, @abort, 2)])

          frame =
            <<correlation_id::32-signed, 0::32, 1::32, 1::16, "t", 1::32, 0::32, 0::16, 3::64, 3::64, 1::32, 42::64,
              0::64, record_set::binary>>

          :ok = :gen_tcp.send(server, frame)
          receive do: (:stop -> :ok)
        end)

      :ok = :gen_tcp.controlling_process(server, responder)
      send(responder, :go)
      responder
    end

    test "a read_committed fetch returns no records yet resumes past the aborted transaction", ctx do
      responder = answer_all_aborted(ctx.server)
      opts = [api_version: 4, isolation_level: 1]

      assert {:reply, {:ok, %Fetch{records: [], last_offset: 2} = fetch}, _state} =
               Client.handle_call({:fetch, "t", 0, 0, opts}, {self(), make_ref()}, ctx.state)

      assert Fetch.next_offset(fetch) == 3
      send(responder, :stop)
    end
  end
end
