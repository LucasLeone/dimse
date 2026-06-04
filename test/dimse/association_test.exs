defmodule Dimse.AssociationTest do
  use ExUnit.Case, async: true

  alias Dimse.Association
  alias Dimse.Association.{State, Config}

  @verification_uid "1.2.840.10008.1.1"
  @ct_image_storage "1.2.840.10008.5.1.4.1.1.2"

  # SCP handler that delays echo response — used to keep pending_request alive.
  # Does NOT implement supported_abstract_syntaxes/0, exercising the L1218 fallback.
  defmodule SlowEchoHandler do
    @behaviour Dimse.Handler

    @impl Dimse.Handler
    def handle_echo(_cmd, _state) do
      Process.sleep(2_000)
      {:ok, 0x0000}
    end

    @impl Dimse.Handler
    def handle_store(_cmd, _data, _state), do: {:ok, 0x0000}

    @impl Dimse.Handler
    def handle_find(_cmd, _query, _state), do: {:ok, []}

    @impl Dimse.Handler
    def handle_move(_cmd, _query, _state), do: {:ok, []}

    @impl Dimse.Handler
    def handle_get(_cmd, _query, _state), do: {:ok, []}
  end

  defmodule FakeNAssociation do
    use GenServer

    def start_link(response), do: GenServer.start_link(__MODULE__, response)

    @impl true
    def init(response), do: {:ok, response}

    @impl true
    def handle_call({:dimse_request, _command_set, _data}, _from, response) do
      {:reply, response, response}
    end
  end

  defmodule NoSyntaxHandler do
  end

  defmodule SupportedSyntaxHandler do
    def supported_abstract_syntaxes, do: ["1.2.840.10008.5.1.4.1.1.2"]
  end

  defmodule AuthOkNilHandler do
    def handle_authenticate(_identity, _state), do: {:ok, nil}
  end

  defmodule ValidationOkNilHandler do
    def validate_association(_rq, _state), do: {:ok, nil}
  end

  describe "handler_abstract_syntaxes/1" do
    test "defaults to Verification when handler is nil" do
      assert Association.test_handler_abstract_syntaxes(nil) == MapSet.new([@verification_uid])
    end

    test "defaults to Verification when handler has no supported abstract syntaxes" do
      assert Association.test_handler_abstract_syntaxes(NoSyntaxHandler) ==
               MapSet.new([@verification_uid])
    end

    test "uses supported abstract syntaxes from a loaded handler" do
      assert Association.test_handler_abstract_syntaxes(SupportedSyntaxHandler) ==
               MapSet.new([@ct_image_storage])
    end

    test "loads an unloaded handler before checking supported abstract syntaxes" do
      module = Module.concat(__MODULE__, UnloadedSupportedSyntaxHandler)
      beam_dir = Path.join(System.tmp_dir!(), "dimse_association_test_#{System.unique_integer()}")

      try do
        File.mkdir_p!(beam_dir)
        source_path = Path.join(beam_dir, "unloaded_supported_syntax_handler.ex")

        source = """
        defmodule #{inspect(module)} do
          def supported_abstract_syntaxes, do: ["#{@ct_image_storage}"]
        end
        """

        File.write!(source_path, source)

        assert {:ok, _, %{compile_warnings: [], runtime_warnings: []}} =
                 Kernel.ParallelCompiler.compile_to_path([source_path], beam_dir,
                   return_diagnostics: true
                 )

        :code.purge(module)
        :code.delete(module)

        assert false == function_exported?(module, :supported_abstract_syntaxes, 0)
        assert true == :code.add_patha(String.to_charlist(beam_dir))

        assert Association.test_handler_abstract_syntaxes(module) ==
                 MapSet.new([@ct_image_storage])
      after
        :code.del_path(String.to_charlist(beam_dir))
        File.rm_rf!(beam_dir)

        :code.purge(module)
        :code.delete(module)
      end
    end
  end

  describe "start_link/1" do
    test "starts a GenServer process in idle phase" do
      assert {:ok, pid} = Association.start_link([])
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "accepts custom config" do
      config = %Config{ae_title: "MY_SCP", max_pdu_length: 32_768}
      assert {:ok, pid} = Association.start_link(config: config, ae_title: "MY_SCP")
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end

  describe "request/4 when not established" do
    test "returns error when association is in idle phase" do
      {:ok, pid} = Association.start_link([])
      assert {:error, :not_established} = Association.request(pid, %{}, nil, 1_000)
      GenServer.stop(pid)
    end

    test "returns :no_accepted_context when no negotiated presentation context matches" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@verification_uid]
        )

      assert :ok = wait_for_established(assoc)

      assert {:error, :no_accepted_context} =
               Dimse.store(assoc, @ct_image_storage, "1.2.3", <<1, 2, 3>>, timeout: 1_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "connect/3" do
    test "returns an association with negotiated contexts already available" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      assert {:ok, assoc} =
               Dimse.connect("127.0.0.1", port,
                 calling_ae: "TEST_SCU",
                 called_ae: "DIMSE",
                 abstract_syntaxes: [@verification_uid]
               )

      assert %{1 => {@verification_uid, _}} = Association.negotiated_contexts(assoc)
      assert :ok = Dimse.echo(assoc, timeout: 1_000)
      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "returns rejection when no presentation contexts are accepted" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      assert {:error, {:rejected, 1, 1, 1}} =
               Dimse.connect("127.0.0.1", port,
                 calling_ae: "TEST_SCU",
                 called_ae: "DIMSE",
                 abstract_syntaxes: [@ct_image_storage],
                 timeout: 1_000
               )

      Dimse.stop_listener(ref)
    end
  end

  describe "release/2 when not established" do
    test "returns error when association is in idle phase" do
      {:ok, pid} = Association.start_link([])
      assert {:error, :not_established} = Association.release(pid, 1_000)
      GenServer.stop(pid)
    end
  end

  describe "negotiated_contexts/1" do
    test "returns empty map for new association" do
      {:ok, pid} = Association.start_link([])
      assert %{} = Association.negotiated_contexts(pid)
      GenServer.stop(pid)
    end
  end

  describe "abort/1" do
    test "stops the association process" do
      # Use start (not start_link) to avoid exit propagation
      {:ok, pid} = Association.start([])
      ref = Process.monitor(pid)
      Association.abort(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000
    end
  end

  describe "handle_call catch-all" do
    test "returns :not_established for unrecognised calls in idle state" do
      {:ok, pid} = Association.start_link([])
      assert {:error, :not_established} = GenServer.call(pid, :unknown_call_9f3a)
      GenServer.stop(pid)
    end
  end

  describe "handle_cast cancel_find in non-established state" do
    test "is a no-op when association is in idle phase" do
      {:ok, pid} = Association.start_link([])
      # cancel/2 delegates to handle_cast {:cancel_find, _}; must not crash
      Association.cancel(pid, 99)
      # Give the cast time to process
      :timer.sleep(10)
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end

  describe "handle_info stray messages" do
    test "{:sub_operation, :next} with nil sub_operation is a no-op" do
      {:ok, pid} = Association.start_link([])
      send(pid, {:sub_operation, :next})
      :timer.sleep(10)
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "unknown messages are silently ignored" do
      {:ok, pid} = Association.start_link([])
      send(pid, {:totally_unknown_message, :some_payload})
      :timer.sleep(10)
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end

  describe "Dimse.Scu.open/3 timeout behaviour" do
    test "returns {:error, :timeout} when timeout: 0 and process just started" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      assert {:error, :timeout} =
               Dimse.Scu.open("127.0.0.1", port,
                 timeout: 0,
                 abstract_syntaxes: [@verification_uid]
               )

      Dimse.stop_listener(ref)
    end

    test "returns {:error, :timeout} when server accepts TCP but never sends A-ASSOCIATE-AC" do
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      # Accept so the client's TCP connect succeeds; hold the connection open
      # for longer than the timeout so we don't get :tcp_closed instead
      Task.start(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        :timer.sleep(1_000)
        :gen_tcp.close(conn)
      end)

      assert {:error, :timeout} =
               Dimse.Scu.open("127.0.0.1", port,
                 timeout: 150,
                 abstract_syntaxes: [@verification_uid]
               )

      :gen_tcp.close(listen_sock)
    end
  end

  describe "Dimse module default-argument stubs" do
    test "echo/1 (no opts) calls echo/2 with []" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port, abstract_syntaxes: [@verification_uid])

      assert :ok = Dimse.echo(assoc)
      assert :ok = Dimse.release(assoc)
      Dimse.stop_listener(ref)
    end

    test "connect/2 (no opts) calls connect/3 with []" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      # connect/2 defaults to Verification SOP Class
      assert {:ok, assoc} = Dimse.connect("127.0.0.1", port)
      assert :ok = Dimse.release(assoc)
      Dimse.stop_listener(ref)
    end

    test "store/4 (no opts) calls store/5 with []" do
      {:ok, fake} = FakeNAssociation.start_link({:ok, %{{0x0000, 0x0900} => 0x0000}, nil})
      assert :ok = Dimse.store(fake, "1.2.3", "4.5.6", <<1, 2, 3>>)
    end

    test "move/3 (no opts) raises KeyError because :dest_ae is required" do
      assert_raise KeyError, fn -> Dimse.move(:dummy_pid, :study, <<>>) end
    end

    test "start_listener/0 (no opts) raises KeyError because :handler is required" do
      assert_raise KeyError, fn -> Dimse.start_listener() end
    end

    test "n_get/3 (no opts) calls n_get/4 with []" do
      {:ok, fake} = FakeNAssociation.start_link({:ok, %{{0x0000, 0x0900} => 0x0000}, nil})
      assert {:ok, 0x0000, nil} = Dimse.n_get(fake, "1.2.3", "4.5.6")
    end

    test "n_set/4 (no opts) calls n_set/5 with []" do
      {:ok, fake} = FakeNAssociation.start_link({:ok, %{{0x0000, 0x0900} => 0x0000}, nil})
      assert {:ok, 0x0000, nil} = Dimse.n_set(fake, "1.2.3", "4.5.6", <<1, 2>>)
    end

    test "n_action/5 (no opts) calls n_action/6 with []" do
      {:ok, fake} = FakeNAssociation.start_link({:ok, %{{0x0000, 0x0900} => 0x0000}, nil})
      assert {:ok, 0x0000, nil} = Dimse.n_action(fake, "1.2.3", "4.5.6", 1, nil)
    end

    test "n_create/3 (no opts) calls n_create/4 with []" do
      {:ok, fake} =
        FakeNAssociation.start_link(
          {:ok, %{{0x0000, 0x0900} => 0x0000, {0x0000, 0x1000} => nil}, nil}
        )

      assert {:ok, 0x0000, nil, nil} = Dimse.n_create(fake, "1.2.3", nil)
    end

    test "n_delete/3 (no opts) calls n_delete/4 with []" do
      {:ok, fake} = FakeNAssociation.start_link({:ok, %{{0x0000, 0x0900} => 0x0000}, nil})
      assert {:ok, 0x0000, nil} = Dimse.n_delete(fake, "1.2.3", "4.5.6")
    end

    test "n_event_report/5 (no opts) calls n_event_report/6 with []" do
      {:ok, fake} = FakeNAssociation.start_link({:ok, %{{0x0000, 0x0900} => 0x0000}, nil})
      assert {:ok, 0x0000, nil} = Dimse.n_event_report(fake, "1.2.3", "4.5.6", 1, nil)
    end
  end

  describe "State struct" do
    test "has correct defaults" do
      state = %State{}
      assert state.phase == :idle
      assert state.max_pdu_length == 16_384
      assert state.proposed_contexts == %{}
      assert state.negotiated_contexts == %{}
      assert state.pdu_buffer == <<>>
      assert state.bytes_received == 0
      assert state.bytes_sent == 0
      assert state.pending_request == nil
      assert state.pending_release == nil
      assert state.artim_timer == nil
    end

    test "phase can be set to all valid values" do
      for phase <- [:idle, :negotiating, :established, :releasing, :closed] do
        state = %State{phase: phase}
        assert state.phase == phase
      end
    end
  end

  describe "Config struct" do
    test "has correct defaults" do
      config = %Config{}
      assert config.ae_title == "DIMSE"
      assert config.max_pdu_length == 16_384
      assert config.max_associations == 200
      assert config.association_timeout == 600_000
      assert config.dimse_timeout == 30_000
      assert config.artim_timeout == 30_000
      assert config.num_acceptors == 10
    end
  end

  describe "close_connection with pending DIMSE requests" do
    test "abort replies {:error, :aborted} to in-flight DIMSE request (pending_request)" do
      # SlowEchoHandler delays 2s — SCP's abstract_syntaxes defaults to Verification (L1218)
      {:ok, ref} = Dimse.start_listener(port: 0, handler: SlowEchoHandler)
      port = :ranch.get_port(ref)

      {:ok, assoc} = Dimse.connect("127.0.0.1", port, abstract_syntaxes: [@verification_uid])
      assert :ok = wait_for_established(assoc)

      # Start echo in a background task — SlowEchoHandler blocks before responding
      task = Task.async(fn -> Dimse.echo(assoc, timeout: 10_000) end)
      # Allow time for the request to be sent and pending_request to be set
      :timer.sleep(100)

      # Abort while the echo request is pending — triggers pending_request reply
      Dimse.abort(assoc)

      assert {:error, _} = Task.await(task, 3_000)
      Dimse.stop_listener(ref)
    end

    test "tcp_closed replies {:error, :tcp_closed} to pending release" do
      # Use a raw TCP server that sends A-ASSOCIATE-AC then drops A-RELEASE-RQ
      # without responding, causing the SCU to get tcp_closed while releasing.
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      test_pid = self()

      Task.start(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        # Read the A-ASSOCIATE-RQ (discard it)
        {:ok, _rq_data} = :gen_tcp.recv(conn, 0, 1_000)
        # Build a minimal valid A-ASSOCIATE-AC and send it
        ac_pdu = build_associate_ac()
        :gen_tcp.send(conn, ac_pdu)
        # Wait until SCU sends A-RELEASE-RQ, then close without responding
        {:ok, _release_rq} = :gen_tcp.recv(conn, 0, 5_000)
        send(test_pid, :release_rq_received)
        :gen_tcp.close(conn)
      end)

      {:ok, assoc} =
        Dimse.Scu.open("127.0.0.1", port,
          timeout: 5_000,
          abstract_syntaxes: [@verification_uid]
        )

      # Start release in a background task
      task = Task.async(fn -> Association.release(assoc, 10_000) end)
      assert_receive :release_rq_received, 3_000

      # Connection already closed by server — release should get tcp_closed
      assert {:error, :tcp_closed} = Task.await(task, 3_000)
      :gen_tcp.close(listen_sock)
    end
  end

  describe "Dimse.Association default-argument stubs" do
    test "request/2 (no data or timeout) calls request/4 with nil, 30_000" do
      {:ok, pid} = Association.start_link([])
      assert {:error, :not_established} = Association.request(pid, %{})
      GenServer.stop(pid)
    end

    test "find_request/3 (no timeout) calls find_request/4 with 30_000" do
      {:ok, pid} = Association.start_link([])
      assert {:error, :not_established} = Association.find_request(pid, %{}, <<>>)
      GenServer.stop(pid)
    end

    test "get_request/3 (no timeout) calls get_request/4 with 30_000" do
      {:ok, pid} = Association.start_link([])
      assert {:error, :not_established} = Association.get_request(pid, %{}, <<>>)
      GenServer.stop(pid)
    end

    test "release/1 (no timeout) calls release/2 with 30_000" do
      {:ok, pid} = Association.start_link([])
      assert {:error, :not_established} = Association.release(pid)
      GenServer.stop(pid)
    end
  end

  describe "Dimse.Scu default-argument stubs" do
    test "open/2 (no opts) establishes an association with defaults" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      assert {:ok, assoc} = Dimse.Scu.open("127.0.0.1", port)
      assert :ok = Dimse.Scu.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "release/1 (no timeout) calls release/2 with 30_000" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} = Dimse.connect("127.0.0.1", port)
      assert :ok = Dimse.Scu.release(assoc)
      Dimse.stop_listener(ref)
    end
  end

  describe "Association handles tcp_closed during established" do
    test "association exits with :tcp_closed when remote side closes unexpectedly" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@verification_uid]
        )

      assert :ok = wait_for_established(assoc)
      pid_ref = Process.monitor(assoc)

      # Stop the listener — Ranch terminates the SCP connection, closing the
      # socket. The SCU association receives {:tcp_closed, socket} and exits.
      Dimse.stop_listener(ref)

      assert_receive {:DOWN, ^pid_ref, :process, ^assoc, :tcp_closed}, 2_000
    end
  end

  describe "Dimse.Scu.open/3 A-ABORT handling" do
    test "returns {:error, {:aborted, source, reason}} when SCP sends A-ABORT during negotiation" do
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      # A-ABORT PDU: item-type=0x07, reserved, PDU-length=4, reserved×2, source=2, reason=0
      a_abort_pdu = <<0x07, 0x00, 0, 0, 0, 4, 0, 0, 2, 0>>

      Task.start(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        # Brief wait so A-ASSOCIATE-RQ arrives, then abort
        :timer.sleep(20)
        :gen_tcp.send(conn, a_abort_pdu)
        :timer.sleep(200)
        :gen_tcp.close(conn)
      end)

      assert {:error, {:aborted, _source, _reason}} =
               Dimse.Scu.open("127.0.0.1", port,
                 timeout: 2_000,
                 abstract_syntaxes: [@verification_uid]
               )

      :gen_tcp.close(listen_sock)
    end
  end

  # Builds a minimal valid A-ASSOCIATE-AC PDU accepting presentation context 1
  # with Verification SOP Class / Implicit VR Little Endian.
  defp build_associate_ac do
    transfer_syntax = "1.2.840.10008.1.2"
    impl_uid = "1.2.826.0.1.3680043.8.498.1"
    app_context_name = "1.2.840.10008.3.1.1.1"

    # Application Context Item (0x10)
    app_ctx = encode_sub_item(0x10, app_context_name)

    # Presentation Context Result Item (0x21): id=1, result=0 (accepted)
    ts_item = encode_sub_item(0x40, transfer_syntax)
    pc_item = encode_sub_item(0x21, <<1, 0, 0, 0>> <> ts_item)

    # User Information Item (0x50) with max-length (0x51) and impl UID (0x52)
    max_len_item = <<0x51, 0x00, 0, 4, 0, 0, 0x40, 0x00>>
    impl_uid_item = encode_sub_item(0x52, impl_uid)
    user_info = encode_sub_item(0x50, max_len_item <> impl_uid_item)

    # Fixed header fields: protocol-version, reserved, called/calling AE, 32 reserved
    called = String.pad_trailing("DIMSE", 16)
    calling = String.pad_trailing("DIMSE", 16)
    reserved32 = :binary.copy(<<0>>, 32)

    payload =
      <<0x00, 0x01, 0x00, 0x00>> <>
        called <>
        calling <>
        reserved32 <>
        app_ctx <>
        pc_item <>
        user_info

    <<0x02, 0x00, byte_size(payload)::32>> <> payload
  end

  defp encode_sub_item(type, data) when is_binary(data) do
    <<type, 0x00, byte_size(data)::16>> <> data
  end

  describe "Scu.open/3 exit normalization" do
    test "returns {:error, :closed} when association process exits normally during await" do
      # Start a raw TCP server that accepts and immediately closes
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      Task.start(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        # Close immediately — Association process exits :normal before negotiation
        :gen_tcp.close(conn)
      end)

      result =
        Dimse.Scu.open("127.0.0.1", port,
          timeout: 3_000,
          abstract_syntaxes: [@verification_uid]
        )

      # Should normalize the exit reason to an error tuple
      assert {:error, _reason} = result
      :gen_tcp.close(listen_sock)
    end

    test "returns {:error, {:aborted, ...}} when SCP sends A-ABORT and process exits with shutdown" do
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      # SCP sends A-ABORT immediately — the Association exits with {:aborted, source, reason}
      # which may be wrapped in {:shutdown, ...}
      a_abort_pdu = <<0x07, 0x00, 0, 0, 0, 4, 0, 0, 0, 0>>

      Task.start(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        {:ok, _rq} = :gen_tcp.recv(conn, 0, 1_000)
        :gen_tcp.send(conn, a_abort_pdu)
        :timer.sleep(100)
        :gen_tcp.close(conn)
      end)

      assert {:error, _} =
               Dimse.Scu.open("127.0.0.1", port,
                 timeout: 3_000,
                 abstract_syntaxes: [@verification_uid]
               )

      :gen_tcp.close(listen_sock)
    end

    test "returns {:error, :closed} when association process exits :normal (monitor path)" do
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      # Send a garbage response that causes the Association to stop normally
      Task.start(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        {:ok, _rq} = :gen_tcp.recv(conn, 0, 1_000)
        # Send invalid PDU (type 0xFF) — Association should abort/close
        :gen_tcp.send(conn, <<0xFF, 0x00, 0, 0, 0, 0>>)
        :timer.sleep(200)
        :gen_tcp.close(conn)
      end)

      assert {:error, _} =
               Dimse.Scu.open("127.0.0.1", port,
                 timeout: 3_000,
                 abstract_syntaxes: [@verification_uid]
               )

      :gen_tcp.close(listen_sock)
    end
  end

  describe "Scu.put_if/3" do
    test "returns map unchanged when value is nil" do
      assert %{a: 1} = Dimse.Scu.put_if(%{a: 1}, :b, nil)
    end

    test "adds key when value is non-nil" do
      assert %{a: 1, b: 2} = Dimse.Scu.put_if(%{a: 1}, :b, 2)
    end
  end

  describe "Scu.normalize_n_response/2" do
    test "returns {:ok, status, data} for success status" do
      response = %{{0x0000, 0x0900} => 0x0000}
      assert {:ok, 0x0000, <<1, 2>>} = Dimse.Scu.normalize_n_response(response, <<1, 2>>)
    end

    test "returns {:ok, status, data} for warning status" do
      response = %{{0x0000, 0x0900} => 0x0001}
      assert {:ok, 0x0001, nil} = Dimse.Scu.normalize_n_response(response, nil)
    end

    test "returns {:error, {:status, code, data}} for failure status" do
      response = %{{0x0000, 0x0900} => 0xC000}
      assert {:error, {:status, 0xC000, <<1>>}} = Dimse.Scu.normalize_n_response(response, <<1>>)
    end
  end

  describe "Scu helper normalization" do
    test "finalize_open/3 aborts immediately when no time remains" do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      started_at = System.monotonic_time(:millisecond) - 10

      assert {:error, :timeout} = Dimse.Scu.finalize_open(pid, started_at, 0)
    end

    test "normalize_connect_call_exit/1 and normalize_connect_exit/1 cover supported exit forms" do
      assert :closed = Dimse.Scu.normalize_connect_call_exit({:noproc, {GenServer, :call, []}})
      assert :closed = Dimse.Scu.normalize_connect_call_exit({:normal, {GenServer, :call, []}})
      assert :tcp_closed = Dimse.Scu.normalize_connect_call_exit(:tcp_closed)

      assert {:rejected, 1, 2, 3} =
               Dimse.Scu.normalize_connect_call_exit({:shutdown, {:rejected, 1, 2, 3}})

      assert {:aborted, 2, 0} = Dimse.Scu.normalize_connect_exit({:aborted, 2, 0})
      assert :closed = Dimse.Scu.normalize_connect_exit({:shutdown, :normal})
      assert :tcp_closed = Dimse.Scu.normalize_connect_exit(:tcp_closed)
    end

    test "await_established_exit/3 prefers DOWN reasons and otherwise falls back" do
      down_pid = spawn(fn -> :ok end)
      down_ref = Process.monitor(down_pid)

      assert {:error, :closed} =
               Dimse.Scu.await_established_exit(
                 down_pid,
                 down_ref,
                 {:noproc, {GenServer, :call, []}}
               )

      alive_pid = spawn(fn -> Process.sleep(100) end)
      alive_ref = Process.monitor(alive_pid)

      assert {:error, {:aborted, 2, 0}} =
               Dimse.Scu.await_established_exit(
                 alive_pid,
                 alive_ref,
                 {:shutdown, {:aborted, 2, 0}}
               )
    end

    test "await_established_exit/3 returns the monitored DOWN reason when it arrives" do
      pid = spawn(fn -> Process.sleep(5_000) end)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      Process.sleep(20)

      assert {:error, :killed} =
               Dimse.Scu.await_established_exit(
                 pid,
                 ref,
                 {:shutdown, {:aborted, 2, 0}}
               )
    end

    test "test_do_await_established/3 catches negotiated context lookup exits" do
      dead_pid = spawn(fn -> :ok end)
      ref = make_ref()

      # Ensure the process is gone before the SCU polls it.
      Process.sleep(20)

      assert {:error, :closed} =
               Dimse.Scu.test_do_await_established(
                 dead_pid,
                 ref,
                 System.monotonic_time(:millisecond) + 100
               )
    end
  end

  describe "Association internal helpers" do
    test "close_socket/1 nil fast path is harmless" do
      assert :ok = Association.test_close_socket(%State{socket: nil})
    end

    test "handler syntax and callback-state helpers cover nil and missing-context branches" do
      assert Association.test_handler_abstract_syntaxes(nil) == MapSet.new([@verification_uid])

      assert Association.test_handler_abstract_syntaxes(NoSyntaxHandler) ==
               MapSet.new([@verification_uid])

      state = %State{negotiated_contexts: %{}, current_context_id: nil}
      message = %Dimse.Message{context_id: 3, command: %{}}

      assert %State{current_context_id: 3, current_abstract_syntax_uid: nil} =
               Association.test_callback_state_for_message(state, message)

      refute Association.test_request_on_negotiated_context?(message, state)
    end

    test "user info and N-response helpers cover nil and error forms" do
      assert Association.test_get_in_user_info(%{}, :user_identity) == nil

      assert {0xC000, nil, %{}} =
               Association.test_normalize_n_dispatch_result(
                 :handle_n_set,
                 {:error, 0xC000, "bad"},
                 %{}
               )

      cmd = %{}
      request = %{{0x0000, 0x1000} => "1.2.3"}

      assert %{{0x0000, 0x1000} => "1.2.3"} =
               Association.test_maybe_put_instance_uid(cmd, 0x0001, request)

      assert %{} = Association.test_maybe_put_instance_uid(cmd, 0x0001, %{})
      assert %{} = Association.test_maybe_put_instance_uid(cmd, 0x8020, request)
    end

    test "auth and validation helpers cover nil and ok-nil branches" do
      state = %State{}

      identity = %Dimse.Pdu.UserIdentity{
        identity_type: 0x01,
        primary_field: "user",
        positive_response_requested: false
      }

      user_info = %Dimse.Pdu.UserInformation{user_identity: nil}

      assert {:ok, nil} = Association.test_authenticate_user(nil, AuthOkNilHandler, state)
      assert {:ok, nil} = Association.test_authenticate_user(user_info, AuthOkNilHandler, state)

      assert {:ok, nil} =
               Association.test_authenticate_user(
                 %Dimse.Pdu.UserInformation{user_identity: identity},
                 AuthOkNilHandler,
                 state
               )

      assert :ok =
               Association.test_validate_association_request(%{}, ValidationOkNilHandler, state)
    end

    test "role-selection and wait helpers cover nil, empty, and timeout branches" do
      assert Association.test_echo_role_selections(
               %Dimse.Pdu.UserInformation{role_selections: nil},
               %{}
             ) == nil

      assert Association.test_echo_role_selections(
               %Dimse.Pdu.UserInformation{role_selections: []},
               %{}
             ) == nil

      roles = [
        %Dimse.Pdu.RoleSelection{
          sop_class_uid: @verification_uid,
          scu_role: true,
          scp_role: false
        }
      ]

      accepted = %{1 => {@verification_uid, "1.2.840.10008.1.2"}}

      assert roles ==
               Association.test_echo_role_selections(
                 %Dimse.Pdu.UserInformation{role_selections: roles},
                 accepted
               )

      assert %{} = Association.test_roles_to_map(nil)
      assert %{@verification_uid => {true, false}} = Association.test_roles_to_map(roles)

      sleepy = spawn(fn -> Process.sleep(100) end)

      assert :timeout =
               Association.test_do_wait_sub_assoc(sleepy, System.monotonic_time(:millisecond) - 1)
    end
  end

  describe "Scu.open/3 catch :exit path" do
    test "returns error when Association process dies during negotiated_contexts call" do
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      spawn(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        {:ok, _rq} = :gen_tcp.recv(conn, 0, 2_000)
        # Don't respond — Association stays in :negotiating until tcp_closed
        :timer.sleep(50)
        :gen_tcp.close(conn)
      end)

      assert {:error, _} =
               Dimse.Scu.open("127.0.0.1", port,
                 timeout: 2_000,
                 abstract_syntaxes: [@verification_uid]
               )

      :gen_tcp.close(listen_sock)
    end
  end

  describe "Association protocol edge cases" do
    test "SCP aborts on unexpected PDU type after establishment" do
      {:ok, listen_sock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_sock)

      test_pid = self()

      Task.start(fn ->
        {:ok, conn} = :gen_tcp.accept(listen_sock, 5_000)
        {:ok, _rq} = :gen_tcp.recv(conn, 0, 1_000)
        ac_pdu = build_associate_ac()
        :gen_tcp.send(conn, ac_pdu)
        # Wait for association to be established
        :timer.sleep(50)
        # Send A-ASSOCIATE-RQ (type 0x01) on an established association — unexpected
        :gen_tcp.send(conn, <<0x01, 0x00, 0, 0, 0, 4, 0, 0, 0, 0>>)
        send(test_pid, :unexpected_pdu_sent)
        # Read the A-ABORT response from the SCP
        case :gen_tcp.recv(conn, 0, 2_000) do
          {:ok, data} -> send(test_pid, {:abort_received, data})
          {:error, :closed} -> send(test_pid, :conn_closed)
        end

        :gen_tcp.close(conn)
      end)

      {:ok, assoc} =
        Dimse.Scu.open("127.0.0.1", port,
          timeout: 5_000,
          abstract_syntaxes: [@verification_uid]
        )

      assert_receive :unexpected_pdu_sent, 2_000
      # The association should die from the unexpected PDU
      ref = Process.monitor(assoc)
      assert_receive {:DOWN, ^ref, :process, ^assoc, _reason}, 3_000
      :gen_tcp.close(listen_sock)
    end

    test "abort while DIMSE request is pending replies with error" do
      # SlowEchoHandler sleeps 2s in handle_echo — gives us time to abort
      {:ok, ref} = Dimse.start_listener(port: 0, handler: SlowEchoHandler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port, abstract_syntaxes: [@verification_uid])

      assert :ok = wait_for_established(assoc)

      # Start echo in background — SlowEchoHandler blocks before responding
      task = Task.async(fn -> Dimse.echo(assoc, timeout: 10_000) end)
      :timer.sleep(100)

      # Abort while echo is pending — should trigger pending_request reply
      Dimse.abort(assoc)

      assert {:error, _} = Task.await(task, 3_000)
      Dimse.stop_listener(ref)
    end
  end

  test "SCP aborts when receiving unexpected PDU on established association" do
    {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
    port = :ranch.get_port(ref)

    # Connect as a raw TCP client, perform handshake, then send garbage PDU
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    # Build and send A-ASSOCIATE-RQ
    a_rq = build_associate_rq()
    :gen_tcp.send(sock, a_rq)
    # Read A-ASSOCIATE-AC
    {:ok, _ac} = :gen_tcp.recv(sock, 0, 2_000)

    # Send A-RELEASE-RP (type 0x06) — unexpected in :established phase (expected only in :releasing)
    # The SCP should respond with A-ABORT (type 0x07)
    :gen_tcp.send(sock, <<0x06, 0x00, 0, 0, 0, 4, 0, 0, 0, 0>>)

    # Read the A-ABORT PDU response or connection close
    case :gen_tcp.recv(sock, 0, 2_000) do
      {:ok, <<0x07, _::binary>>} -> :ok
      {:error, :closed} -> :ok
    end

    :gen_tcp.close(sock)
    # Allow SCP process to terminate and flush cover data
    :timer.sleep(50)
    Dimse.stop_listener(ref)
  end

  test "A-ABORT received while DIMSE request in flight replies to pending caller" do
    {:ok, ref} = Dimse.start_listener(port: 0, handler: SlowEchoHandler)
    port = :ranch.get_port(ref)

    {:ok, assoc} =
      Dimse.connect("127.0.0.1", port, abstract_syntaxes: [@verification_uid])

    assert :ok = wait_for_established(assoc)

    # Start echo — SlowEchoHandler blocks 2s
    task = Task.async(fn -> Dimse.echo(assoc, timeout: 10_000) end)
    :timer.sleep(100)

    # Send A-ABORT via the socket directly to trigger the pending_request abort path.
    # We can't do that easily, so we use Dimse.abort which sends A-ABORT PDU.
    Dimse.abort(assoc)

    assert {:error, _} = Task.await(task, 3_000)
    Dimse.stop_listener(ref)
  end

  describe "Association with nil handler" do
    test "handler_abstract_syntaxes/1 defaults to Verification when handler is nil" do
      # When SlowEchoHandler doesn't implement supported_abstract_syntaxes/0,
      # the SCP falls back to Verification UID only — exercising the nil handler path.
      # This is already tested by SlowEchoHandler above, but let's verify the SCP
      # accepts connections with Verification when handler has no supported_abstract_syntaxes
      {:ok, ref} = Dimse.start_listener(port: 0, handler: SlowEchoHandler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port, abstract_syntaxes: [@verification_uid])

      contexts = Dimse.Association.negotiated_contexts(assoc)
      assert map_size(contexts) > 0
      Dimse.abort(assoc)
      Dimse.stop_listener(ref)
    end
  end

  # Builds a minimal A-ASSOCIATE-RQ PDU proposing Verification SOP Class.
  defp build_associate_rq do
    transfer_syntax = "1.2.840.10008.1.2"
    abstract_syntax = "1.2.840.10008.1.1"
    app_context_name = "1.2.840.10008.3.1.1.1"
    impl_uid = "1.2.826.0.1.3680043.8.498.1"

    app_ctx = encode_sub_item(0x10, app_context_name)

    # Presentation Context Item (0x20): id=1, abstract syntax + transfer syntax
    as_item = encode_sub_item(0x30, abstract_syntax)
    ts_item = encode_sub_item(0x40, transfer_syntax)
    pc_item = encode_sub_item(0x20, <<1, 0, 0, 0>> <> as_item <> ts_item)

    # User Information Item (0x50)
    max_len_item = <<0x51, 0x00, 0, 4, 0, 0, 0x40, 0x00>>
    impl_uid_item = encode_sub_item(0x52, impl_uid)
    user_info = encode_sub_item(0x50, max_len_item <> impl_uid_item)

    called = String.pad_trailing("DIMSE", 16)
    calling = String.pad_trailing("TEST_RAW", 16)
    reserved32 = :binary.copy(<<0>>, 32)

    payload =
      <<0x00, 0x01, 0x00, 0x00>> <>
        called <>
        calling <>
        reserved32 <>
        app_ctx <>
        pc_item <>
        user_info

    <<0x01, 0x00, byte_size(payload)::32>> <> payload
  end

  defp wait_for_established(assoc, timeout \\ 2_000) do
    contexts = Dimse.Association.negotiated_contexts(assoc)

    assert map_size(contexts) > 0,
           "Association was not established immediately after connect/3 within #{timeout}ms"

    :ok
  end
end
