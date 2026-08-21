defmodule KafkaEx.Network.NetworkClient do
  @moduledoc """
  KafkaEx implementation of Client used to connect to Kafka Broker
  """
  @behaviour KafkaEx.Network.Behaviour
  require Logger

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Auth.SASL
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Network.Socket
  alias KafkaEx.Telemetry

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth_opts \\ nil) do
    metadata = Telemetry.connection_metadata(host, port, use_ssl)

    Telemetry.span([:kafka_ex, :connection], metadata, fn ->
      do_create_socket(host, port, ssl_options, use_ssl, auth_opts)
    end)
  end

  defp do_create_socket(host, port, ssl_options, use_ssl, auth_opts) do
    case Socket.create(format_host(host), port, build_socket_options(ssl_options), use_ssl) do
      {:ok, socket} ->
        finish_socket_setup(socket, enrich_auth_opts(auth_opts, host, port), host, port)

      {:error, reason} ->
        Logger.error("Could not connect to #{inspect_broker(host, port)} because of error #{inspect(reason)}")
        {nil, %{success: false, error: reason}}
    end
  end

  defp finish_socket_setup(socket, auth_opts, host, port) do
    case do_authenticate_sasl(socket, auth_opts, host, port) do
      :ok ->
        :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
        {socket, %{success: true}}

      {:error, reason} ->
        close_socket(%{host: host, port: port}, socket, :auth_error)
        Logger.error("SASL authentication failed for #{inspect_broker(host, port)}}: #{inspect(reason)}")
        {nil, %{success: false, error: reason}}
    end
  end

  defp do_authenticate_sasl(_socket, nil, _host, _port), do: :ok

  defp do_authenticate_sasl(socket, %AuthConfig{} = cfg, host, port) do
    mechanism = get_mechanism_name(cfg)
    metadata = Telemetry.auth_metadata(host, port, mechanism)

    Telemetry.span([:kafka_ex, :auth], metadata, fn ->
      result = maybe_authenticate_sasl(socket, cfg)
      {result, %{}}
    end)
  end

  defp get_mechanism_name(%AuthConfig{mechanism: :plain}), do: "PLAIN"
  defp get_mechanism_name(%AuthConfig{mechanism: :scram, mechanism_opts: %{algo: :sha512}}), do: "SCRAM-SHA-512"
  defp get_mechanism_name(%AuthConfig{mechanism: :scram}), do: "SCRAM-SHA-256"
  defp get_mechanism_name(%AuthConfig{mechanism: :oauthbearer}), do: "OAUTHBEARER"
  defp get_mechanism_name(%AuthConfig{mechanism: mech}), do: to_string(mech)

  @impl true
  def close_socket(nil), do: :ok
  def close_socket(socket), do: Socket.close(socket)

  @doc """
  Closes a socket and emits telemetry event.

  This is the preferred way to close sockets when the broker context is available,
  as it emits `[:kafka_ex, :connection, :close]` telemetry event with the close reason.

  ## Parameters
    - `broker` - Broker struct or map with `:host` and `:port` keys
    - `socket` - The socket to close (nil is a no-op)
    - `reason` - Why the socket is being closed (e.g., :shutdown, :timeout, :send_error)
  """
  @spec close_socket(map(), any(), atom()) :: :ok
  def close_socket(_broker, nil, _reason), do: :ok

  def close_socket(%{host: host, port: port}, socket, reason) do
    Telemetry.emit_connection_close(host, port, reason)
    Socket.close(socket)
  end

  @impl true
  def send_async_request(%{socket: nil}, _data) do
    {:error, :not_connected}
  end

  def send_async_request(broker, data) do
    socket = broker.socket

    case Socket.send(socket, data) do
      :ok ->
        :ok

      {_, reason} ->
        broker_str = inspect_broker(broker.host, broker.port)
        Logger.error("Asynchronously sending data to broker #{broker_str} failed with #{inspect(reason)}")
        reason
    end
  end

  @impl true
  def send_sync_request(broker, data, timeout), do: do_send_sync_request(broker, data, timeout, nil, nil)

  @doc """
  Sends `data` and reads frames until the one carrying `correlation_id`.

  Frames read before it are the responses of requests pipelined earlier on the
  same socket: each is passed to `on_earlier_frame` in connection order, so the
  caller can route it to its own requester. Use this instead of
  `send_sync_request/3` whenever the socket has pipelined requests
  outstanding — the passive read would otherwise return their responses.
  """
  @spec send_sync_request_pipelined(map() | nil, iodata(), timeout(), integer(), (binary() -> any())) ::
          binary() | {:error, any()}
  def send_sync_request_pipelined(broker, data, timeout, correlation_id, on_earlier_frame) do
    do_send_sync_request(broker, data, timeout, correlation_id, on_earlier_frame)
  end

  defp do_send_sync_request(%{socket: nil}, _data, _timeout, _correlation_id, _on_earlier_frame) do
    {:error, :not_connected}
  end

  defp do_send_sync_request(%{:socket => socket} = broker, data, timeout, correlation_id, on_earlier_frame) do
    case Socket.setopts(socket, [:binary, {:packet, 4}, {:active, false}]) do
      :ok ->
        response =
          case Socket.send(socket, data) do
            :ok ->
              receive_response(socket, timeout, broker, correlation_id, on_earlier_frame)

            {_, reason} ->
              broker_str = inspect_broker(broker.host, broker.port)
              Logger.error("Sending data to broker #{broker_str} failed with #{inspect(reason)}")
              close_socket(broker, socket, :send_error)
              {:error, reason}
          end

        response

      {:error, reason} ->
        broker_str = inspect_broker(broker.host, broker.port)
        Logger.error("Setting socket options for broker #{broker_str} failed with #{inspect(reason)}")
        close_socket(broker, socket, :send_error)
        {:error, reason}
    end
  end

  defp do_send_sync_request(nil, _, _, _correlation_id, _on_earlier_frame) do
    {:error, :no_broker}
  end

  defp receive_response(socket, timeout, broker, correlation_id, on_earlier_frame) do
    case Socket.recv(socket, 0, timeout) do
      {:ok, data} ->
        if own_response?(data, correlation_id) do
          :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
          data
        else
          on_earlier_frame.(data)
          receive_response(socket, timeout, broker, correlation_id, on_earlier_frame)
        end

      {:error, :timeout} ->
        Logger.error("Receiving data from #{inspect_broker(broker.host, broker.port)} timed out")
        close_socket(broker, socket, :timeout)
        {:error, :timeout}

      {:error, reason} ->
        Logger.error("Receiving data from #{inspect_broker(broker.host, broker.port)} failed with #{inspect(reason)}")
        close_socket(broker, socket, :recv_error)
        {:error, reason}
    end
  end

  # Every Kafka response opens with the correlation id of its request. Without
  # pipelining the first frame is always the answer, so no id is asked for.
  defp own_response?(_data, nil), do: true
  defp own_response?(<<correlation_id::32-signed, _rest::binary>>, correlation_id), do: true
  defp own_response?(_data, _correlation_id), do: false

  @impl true
  def format_host(host) do
    case Regex.scan(~r/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/, host) do
      [match_data] = [[_, _, _, _, _]] ->
        match_data |> tl() |> List.flatten() |> Enum.map(&String.to_integer/1) |> List.to_tuple()

      _ ->
        String.to_charlist(host)
    end
  end

  defp maybe_authenticate_sasl(socket, %AuthConfig{} = cfg) do
    case VersionSupport.validate_config(cfg, socket) do
      :ok -> SASL.authenticate(socket, cfg)
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_socket_options([]), do: [:binary, {:packet, 4}, {:active, false}]
  defp build_socket_options(ssl_options), do: build_socket_options([]) ++ ssl_options

  defp inspect_broker(host, port), do: "broker #{inspect(host)}:#{inspect(port)}"

  defp enrich_auth_opts(nil, _host, _port), do: nil

  defp enrich_auth_opts(%AuthConfig{mechanism_opts: opts} = cfg, host, port) do
    %{cfg | mechanism_opts: Map.merge(opts || %{}, %{broker_host: host, broker_port: port})}
  end
end
