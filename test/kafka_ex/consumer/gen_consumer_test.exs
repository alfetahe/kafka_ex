defmodule KafkaEx.Consumer.GenConsumerTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Test.MockClient

  # non-integration GenConsumer tests

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer

    def handle_message_set(_, state), do: state
  end

  test "calling handle_call raises an error if there is no implementation" do
    assert_raise RuntimeError, fn -> TestConsumer.handle_call(nil, nil, nil) end
  end

  test "calling handle_cast raises an error if there is no implementation" do
    assert_raise RuntimeError, fn -> TestConsumer.handle_cast(nil, nil) end
  end

  test "a fetch whose records were all filtered out moves the consumer past them" do
    test_pid = self()

    # At offset 2 a read_committed fetch saw only an aborted transaction's
    # marker: no records, yet it covered offset 2.
    fetch = fn
      2 ->
        {:ok, Fetch.build(topic: "t", partition: 0, records: [], high_watermark: 3) |> Map.put(:last_offset, 2)}

      offset ->
        send(test_pid, {:fetched, offset})
        {:ok, Fetch.build(topic: "t", partition: 0, records: [], high_watermark: 3)}
    end

    {:ok, client} =
      MockClient.start_link(%{
        fetch: fetch,
        offset_fetch: {:ok, [%{partition_offsets: [%{offset: 2, error_code: :no_error}]}]}
      })

    {:ok, consumer} = GenConsumer.start_link(TestConsumer, "group", "t", 0, client: client)

    assert_receive {:fetched, 3}, 1_000
    GenServer.stop(consumer)
  end

  test "calling handle_info raises an error if there is no implementation" do
    assert capture_log(fn ->
             TestConsumer.handle_info(nil, nil)
           end) =~ "unexpected message in handle_info"
  end
end
