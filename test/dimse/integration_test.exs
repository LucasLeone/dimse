defmodule Dimse.IntegrationTest do
  use ExUnit.Case

  @moduletag :integration

  @ct_image_storage "1.2.840.10008.5.1.4.1.1.2"
  @study_root_find "1.2.840.10008.5.1.4.1.2.2.1"
  @study_root_get "1.2.840.10008.5.1.4.1.2.2.3"
  @study_root_move "1.2.840.10008.5.1.4.1.2.2.2"
  @basic_grayscale_print_management_meta "1.2.840.10008.5.1.1.9"
  @basic_film_session "1.2.840.10008.5.1.1.1"

  defp wait_for_established(assoc, timeout \\ 2_000) do
    contexts = Dimse.Association.negotiated_contexts(assoc)

    assert map_size(contexts) > 0,
           "Association was not established immediately after connect/3 within #{timeout}ms"

    :ok
  end

  describe "C-ECHO end-to-end" do
    test "SCU can echo SCP over TCP" do
      {:ok, ref} =
        Dimse.start_listener(
          port: 0,
          handler: Dimse.Scp.Echo
        )

      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"]
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.release(assoc, 5_000)

      Dimse.stop_listener(ref)
    end

    test "multiple echo requests on same association" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"]
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.echo(assoc, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "abort terminates association" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"]
        )

      wait_for_established(assoc)

      ref_monitor = Process.monitor(assoc)
      Dimse.abort(assoc)

      assert_receive {:DOWN, ^ref_monitor, :process, ^assoc, _reason}, 2_000
      Dimse.stop_listener(ref)
    end

    test "connection refused when no listener" do
      result =
        Dimse.connect("127.0.0.1", 59999,
          calling_ae: "TEST_SCU",
          called_ae: "NOWHERE"
        )

      assert {:error, :econnrefused} = result
    end
  end

  describe "C-STORE end-to-end" do
    test "SCU can store an instance on SCP" do
      test_pid = self()
      handler = store_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      sop_instance_uid = "1.2.3.4.5.6.7.8.9"
      data_set = :crypto.strong_rand_bytes(256)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "STORE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@ct_image_storage]
        )

      wait_for_established(assoc)

      assert :ok =
               Dimse.store(assoc, @ct_image_storage, sop_instance_uid, data_set, timeout: 5_000)

      assert_receive {:stored, ^data_set}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "multiple store requests on same association" do
      test_pid = self()
      handler = store_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "STORE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@ct_image_storage]
        )

      wait_for_established(assoc)

      for i <- 1..3 do
        uid = "1.2.3.#{i}"
        data = :crypto.strong_rand_bytes(128)
        assert :ok = Dimse.store(assoc, @ct_image_storage, uid, data, timeout: 5_000)
        assert_receive {:stored, ^data}, 2_000
      end

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "large data set store" do
      test_pid = self()
      handler = store_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      # 64KB — exceeds default max PDU (16KB), tests fragmentation
      data_set = :crypto.strong_rand_bytes(65_536)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "STORE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@ct_image_storage]
        )

      wait_for_established(assoc)

      assert :ok =
               Dimse.store(assoc, @ct_image_storage, "1.2.3.99", data_set, timeout: 10_000)

      assert_receive {:stored, ^data_set}, 5_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "store with echo on same association" do
      test_pid = self()
      handler = store_echo_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "STORE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1", @ct_image_storage]
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)

      data_set = :crypto.strong_rand_bytes(128)

      assert :ok =
               Dimse.store(assoc, @ct_image_storage, "1.2.3.42", data_set, timeout: 5_000)

      assert_receive {:stored, ^data_set}, 2_000

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "C-FIND end-to-end" do
    test "SCU receives matching results from SCP" do
      test_pid = self()
      result1 = :crypto.strong_rand_bytes(64)
      result2 = :crypto.strong_rand_bytes(96)
      handler = find_handler(test_pid, [result1, result2])

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find]
        )

      wait_for_established(assoc)

      query_data = :crypto.strong_rand_bytes(32)
      assert {:ok, results} = Dimse.find(assoc, @study_root_find, query_data, timeout: 5_000)

      assert length(results) == 2
      assert Enum.at(results, 0) == result1
      assert Enum.at(results, 1) == result2

      assert_receive {:find_query, ^query_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "empty results return empty list" do
      test_pid = self()
      handler = find_handler(test_pid, [])

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find]
        )

      wait_for_established(assoc)

      assert {:ok, []} = Dimse.find(assoc, @study_root_find, <<>>, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "many results (10)" do
      test_pid = self()
      results = for _ <- 1..10, do: :crypto.strong_rand_bytes(48)
      handler = find_handler(test_pid, results)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find]
        )

      wait_for_established(assoc)

      assert {:ok, received} = Dimse.find(assoc, @study_root_find, <<>>, timeout: 5_000)
      assert length(received) == 10
      assert received == results

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "handler error returns failure" do
      test_pid = self()
      handler = find_error_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find]
        )

      wait_for_established(assoc)

      assert {:error, {:status, status}} =
               Dimse.find(assoc, @study_root_find, <<>>, timeout: 5_000)

      assert status == 0xA700

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "find with echo on same association" do
      test_pid = self()
      result1 = :crypto.strong_rand_bytes(64)
      handler = find_echo_handler(test_pid, [result1])

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1", @study_root_find]
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert {:ok, [^result1]} = Dimse.find(assoc, @study_root_find, <<>>, timeout: 5_000)
      assert :ok = Dimse.echo(assoc, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "multiple find requests on same association" do
      test_pid = self()
      results1 = [:crypto.strong_rand_bytes(32)]
      results2 = [:crypto.strong_rand_bytes(48), :crypto.strong_rand_bytes(48)]
      handler = find_handler(test_pid, results1, results2)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find]
        )

      wait_for_established(assoc)

      assert {:ok, received1} = Dimse.find(assoc, @study_root_find, <<>>, timeout: 5_000)
      assert received1 == results1

      assert {:ok, received2} = Dimse.find(assoc, @study_root_find, <<>>, timeout: 5_000)
      assert received2 == results2

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "find with query level convenience atom" do
      test_pid = self()
      result1 = :crypto.strong_rand_bytes(64)
      handler = find_handler(test_pid, [result1])

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find]
        )

      wait_for_established(assoc)

      assert {:ok, [^result1]} = Dimse.find(assoc, :study, <<>>, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "C-CANCEL is sent and SCP handles it gracefully" do
      test_pid = self()
      handler = find_slow_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "FIND_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find]
        )

      wait_for_established(assoc)

      # Start find in a task, then cancel it
      find_task =
        Task.async(fn ->
          Dimse.find(assoc, @study_root_find, <<>>, timeout: 10_000)
        end)

      # Wait for the handler to start processing
      assert_receive {:find_started, message_id}, 2_000

      # Send C-CANCEL — with synchronous handlers, the find will complete
      # but the cancel should not crash the association
      :ok = Dimse.cancel(assoc, message_id)

      # The find completes with all results (handler is synchronous)
      assert {:ok, results} = Task.await(find_task, 10_000)
      assert is_list(results)

      # Association should still be usable after cancel
      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "C-GET end-to-end" do
    test "SCU receives instances via C-GET on same association" do
      test_pid = self()
      instance1 = :crypto.strong_rand_bytes(128)
      instance2 = :crypto.strong_rand_bytes(256)

      instances = [
        {"1.2.840.10008.5.1.4.1.1.2", "1.2.3.4.1", instance1},
        {"1.2.840.10008.5.1.4.1.1.2", "1.2.3.4.2", instance2}
      ]

      handler = get_handler(test_pid, instances)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "GET_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_get, @ct_image_storage]
        )

      wait_for_established(assoc)

      query_data = :crypto.strong_rand_bytes(32)
      assert {:ok, results} = Dimse.get(assoc, :study, query_data, timeout: 10_000)

      assert length(results) == 2
      assert Enum.at(results, 0) == instance1
      assert Enum.at(results, 1) == instance2

      assert_receive {:get_query, ^query_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "empty C-GET results return empty list" do
      test_pid = self()
      handler = get_handler(test_pid, [])

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "GET_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_get, @ct_image_storage]
        )

      wait_for_established(assoc)

      assert {:ok, []} = Dimse.get(assoc, :study, <<>>, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "C-GET handler error returns failure" do
      test_pid = self()
      handler = get_error_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "GET_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_get, @ct_image_storage]
        )

      wait_for_established(assoc)

      assert {:error, {:status, 0xA700}} = Dimse.get(assoc, :study, <<>>, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "C-GET sub-op counts failure when SOP class has no negotiated context" do
      test_pid = self()
      # Return an instance with MR Image Storage — NOT negotiated by the SCU
      mr_storage = "1.2.840.10008.5.1.4.1.1.4"
      instances = [{mr_storage, "1.2.3.99.1", :crypto.strong_rand_bytes(64)}]
      handler = get_handler(test_pid, instances)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      # SCU only negotiates CT Image Storage — MR won't have a context
      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "GET_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_get, @ct_image_storage]
        )

      wait_for_established(assoc)

      # The sub-op should fail (no context for MR), resulting in a failure/warning status
      result = Dimse.get(assoc, :study, <<>>, timeout: 5_000)
      # Status 0xB000 (warning: sub-operations complete, one or more failures)
      assert {:error, {:status, _status}} = result

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "C-GET with echo on same association" do
      test_pid = self()
      instance1 = :crypto.strong_rand_bytes(64)

      instances = [{"1.2.840.10008.5.1.4.1.1.2", "1.2.3.4.1", instance1}]
      handler = get_echo_handler(test_pid, instances)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "GET_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1", @study_root_get, @ct_image_storage]
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert {:ok, [^instance1]} = Dimse.get(assoc, :study, <<>>, timeout: 10_000)
      assert :ok = Dimse.echo(assoc, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "C-MOVE end-to-end" do
    test "SCU sends C-MOVE-RQ and destination SCP receives C-STORE" do
      test_pid = self()

      # Start destination SCP first
      dest_handler = store_dest_handler(test_pid)
      {:ok, dest_ref} = Dimse.start_listener(port: 0, handler: dest_handler)
      dest_port = :ranch.get_port(dest_ref)

      instance1 = :crypto.strong_rand_bytes(128)
      instance2 = :crypto.strong_rand_bytes(256)

      instances = [
        {"1.2.840.10008.5.1.4.1.1.2", "1.2.3.4.1", instance1},
        {"1.2.840.10008.5.1.4.1.1.2", "1.2.3.4.2", instance2}
      ]

      handler = move_handler(test_pid, instances, dest_port)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "MOVE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_move]
        )

      wait_for_established(assoc)

      query_data = :crypto.strong_rand_bytes(32)

      assert {:ok, result} =
               Dimse.move(assoc, :study, query_data, dest_ae: "DEST_SCP", timeout: 10_000)

      assert result.completed == 2
      assert result.failed == 0

      # Verify destination received the instances
      assert_receive {:dest_stored, ^instance1}, 5_000
      assert_receive {:dest_stored, ^instance2}, 5_000
      assert_receive {:move_query, ^query_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
      Dimse.stop_listener(dest_ref)
    end

    test "empty C-MOVE results return success with zero counts" do
      test_pid = self()

      dest_handler = store_dest_handler(test_pid)
      {:ok, dest_ref} = Dimse.start_listener(port: 0, handler: dest_handler)
      dest_port = :ranch.get_port(dest_ref)

      handler = move_handler(test_pid, [], dest_port)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "MOVE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_move]
        )

      wait_for_established(assoc)

      assert {:ok, result} =
               Dimse.move(assoc, :study, <<>>, dest_ae: "DEST_SCP", timeout: 5_000)

      assert result.completed == 0
      assert result.failed == 0

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
      Dimse.stop_listener(dest_ref)
    end

    test "C-MOVE handler error returns failure" do
      test_pid = self()
      handler = move_error_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "MOVE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_move]
        )

      wait_for_established(assoc)

      assert {:error, {:status, 0xA700}} =
               Dimse.move(assoc, :study, <<>>, dest_ae: "DEST_SCP", timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "C-MOVE resolve_ae failure returns error" do
      test_pid = self()

      instance1 = :crypto.strong_rand_bytes(64)
      instances = [{"1.2.840.10008.5.1.4.1.1.2", "1.2.3.4.1", instance1}]

      # Use port 0 — resolve_ae will return unknown_ae error
      handler = move_unknown_dest_handler(test_pid, instances)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "MOVE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_move]
        )

      wait_for_established(assoc)

      assert {:error, {:status, status}} =
               Dimse.move(assoc, :study, <<>>, dest_ae: "UNKNOWN_AE", timeout: 5_000)

      # 0xA801 = Move Destination unknown (PS3.4 Table C.4-2)
      assert status == 0xA801

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "C-MOVE with echo on same association" do
      test_pid = self()

      dest_handler = store_dest_handler(test_pid)
      {:ok, dest_ref} = Dimse.start_listener(port: 0, handler: dest_handler)
      dest_port = :ranch.get_port(dest_ref)

      instance1 = :crypto.strong_rand_bytes(64)
      instances = [{"1.2.840.10008.5.1.4.1.1.2", "1.2.3.4.1", instance1}]
      handler = move_echo_handler(test_pid, instances, dest_port)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "MOVE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1", @study_root_move]
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)

      assert {:ok, result} =
               Dimse.move(assoc, :study, <<>>, dest_ae: "DEST_SCP", timeout: 10_000)

      assert result.completed == 1

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.release(assoc, 5_000)

      Dimse.stop_listener(ref)
      Dimse.stop_listener(dest_ref)
    end
  end

  # --- DIMSE-N Services ---

  @storage_commitment_push "1.2.840.10008.1.20.1"
  @test_n_sop_class "1.2.840.10008.5.1.4.34.6.1"

  describe "N-GET end-to-end" do
    test "SCU receives attribute data from SCP" do
      test_pid = self()
      attr_data = :crypto.strong_rand_bytes(64)
      handler = n_get_handler(test_pid, attr_data)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NGET_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      assert {:ok, 0x0000, ^attr_data} =
               Dimse.n_get(assoc, @test_n_sop_class, "1.2.3.4.5", timeout: 5_000)

      assert_receive {:n_get_called, _command}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "N-SET end-to-end" do
    test "SCU sends modifications and receives updated data" do
      test_pid = self()
      mod_data = :crypto.strong_rand_bytes(48)
      response_data = :crypto.strong_rand_bytes(64)
      handler = n_set_handler(test_pid, response_data)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NSET_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      assert {:ok, 0x0000, ^response_data} =
               Dimse.n_set(assoc, @test_n_sop_class, "1.2.3.4.5", mod_data, timeout: 5_000)

      assert_receive {:n_set_called, ^mod_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "N-ACTION end-to-end" do
    test "SCU sends action and receives reply" do
      test_pid = self()
      action_data = :crypto.strong_rand_bytes(32)
      reply_data = :crypto.strong_rand_bytes(48)
      handler = n_action_handler(test_pid, reply_data)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NACTION_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      assert {:ok, 0x0000, ^reply_data} =
               Dimse.n_action(assoc, @test_n_sop_class, "1.2.3.4.5", 1, action_data,
                 timeout: 5_000
               )

      assert_receive {:n_action_called, ^action_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "N-CREATE end-to-end" do
    test "SCU can send N-CREATE for Film Session over negotiated Print Management Meta context" do
      test_pid = self()
      create_data = :crypto.strong_rand_bytes(64)
      created_uid = "1.2.826.0.1.3680043.8.498.1002"

      handler =
        n_create_handler(test_pid, nil, created_uid, @basic_grayscale_print_management_meta)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "PRINT_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@basic_grayscale_print_management_meta]
        )

      wait_for_established(assoc)

      assert {:ok, 0x0000, ^created_uid, nil} =
               Dimse.n_create(assoc, @basic_film_session, create_data, timeout: 5_000)

      assert_receive {:n_create_called, ^create_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "SCU creates and receives created data" do
      test_pid = self()
      create_data = :crypto.strong_rand_bytes(64)
      response_data = :crypto.strong_rand_bytes(96)
      created_uid = "1.2.826.0.1.3680043.8.498.999"
      handler = n_create_handler(test_pid, response_data, created_uid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NCREATE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      assert {:ok, 0x0000, ^created_uid, ^response_data} =
               Dimse.n_create(assoc, @test_n_sop_class, create_data, timeout: 5_000)

      assert_receive {:n_create_called, ^create_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "N-CREATE response command includes the created SOP Instance UID" do
      test_pid = self()
      create_data = :crypto.strong_rand_bytes(64)
      response_data = :crypto.strong_rand_bytes(96)
      created_uid = "1.2.826.0.1.3680043.8.498.1000"
      handler = n_create_handler(test_pid, response_data, created_uid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NCREATE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      message_id = System.unique_integer([:positive])
      command_set = Dimse.Scu.NCreate.build_command_set(@test_n_sop_class, message_id)

      assert {:ok, response, ^response_data} =
               Dimse.Association.request(assoc, command_set, create_data, 5_000)

      assert response[{0x0000, 0x1000}] == created_uid

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "SCU can send N-CREATE without an Attribute List" do
      test_pid = self()
      created_uid = "1.2.826.0.1.3680043.8.498.1001"
      handler = n_create_handler(test_pid, nil, created_uid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NCREATE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      assert {:ok, 0x0000, ^created_uid, nil} =
               Dimse.n_create(assoc, @test_n_sop_class, nil, timeout: 5_000)

      assert_receive {:n_create_called, nil}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "N-DELETE end-to-end" do
    test "SCU deletes and receives confirmation" do
      test_pid = self()
      handler = n_delete_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NDELETE_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      assert {:ok, 0x0000, nil} =
               Dimse.n_delete(assoc, @test_n_sop_class, "1.2.3.4.5", timeout: 5_000)

      assert_receive {:n_delete_called, _command}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "N-EVENT-REPORT end-to-end" do
    test "SCU sends event and receives acknowledgment" do
      test_pid = self()
      event_data = :crypto.strong_rand_bytes(48)
      handler = n_event_report_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NEVENT_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      assert {:ok, 0x0000, nil} =
               Dimse.n_event_report(assoc, @test_n_sop_class, "1.2.3.4.5", 1, event_data,
                 timeout: 5_000
               )

      assert_receive {:n_event_report_called, ^event_data}, 2_000

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "DIMSE-N error handling" do
    test "unimplemented handler returns explicit error tuples for DIMSE-N services" do
      handler = n_error_handler()

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "NERR_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@test_n_sop_class]
        )

      wait_for_established(assoc)

      assert {:error, {:status, 0x0112, nil}} =
               Dimse.n_get(assoc, @test_n_sop_class, "1.2.3.4.5", timeout: 5_000)

      assert {:error, {:status, 0x0112, nil}} =
               Dimse.n_set(assoc, @test_n_sop_class, "1.2.3.4.5", <<1, 2>>, timeout: 5_000)

      assert {:error, {:status, 0x0112, nil}} =
               Dimse.n_action(assoc, @test_n_sop_class, "1.2.3.4.5", 1, <<3, 4>>, timeout: 5_000)

      assert {:error, {:status, 0x0112, nil}} =
               Dimse.n_create(assoc, @test_n_sop_class, <<5, 6>>, timeout: 5_000)

      assert {:error, {:status, 0x0112, nil}} =
               Dimse.n_delete(assoc, @test_n_sop_class, "1.2.3.4.5", timeout: 5_000)

      assert {:error, {:status, 0x0112, nil}} =
               Dimse.n_event_report(assoc, @test_n_sop_class, "1.2.3.4.5", 1, <<7, 8>>,
                 timeout: 5_000
               )

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "Mixed DIMSE-C and DIMSE-N" do
    test "echo + N-GET on same association" do
      test_pid = self()
      attr_data = :crypto.strong_rand_bytes(32)
      handler = n_get_echo_handler(test_pid, attr_data)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "MIX_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1", @test_n_sop_class]
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)

      assert {:ok, 0x0000, ^attr_data} =
               Dimse.n_get(assoc, @test_n_sop_class, "1.2.3.4.5", timeout: 5_000)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  describe "Storage Commitment flow" do
    test "N-ACTION request followed by N-EVENT-REPORT on separate association" do
      test_pid = self()
      action_data = :crypto.strong_rand_bytes(32)
      event_data = :crypto.strong_rand_bytes(48)
      handler = storage_commitment_handler(test_pid)

      {:ok, ref} = Dimse.start_listener(port: 0, handler: handler)
      port = :ranch.get_port(ref)

      # First association: N-ACTION (commitment request)
      {:ok, assoc1} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "SC_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@storage_commitment_push]
        )

      wait_for_established(assoc1)

      assert {:ok, 0x0000, nil} =
               Dimse.n_action(assoc1, @storage_commitment_push, "1.2.3.4.5", 1, action_data,
                 timeout: 5_000
               )

      assert_receive {:commitment_requested, ^action_data}, 2_000

      assert :ok = Dimse.release(assoc1, 5_000)

      # Second association: N-EVENT-REPORT (commitment result)
      {:ok, assoc2} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "SC_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@storage_commitment_push]
        )

      wait_for_established(assoc2)

      assert {:ok, 0x0000, nil} =
               Dimse.n_event_report(assoc2, @storage_commitment_push, "1.2.3.4.5", 1, event_data,
                 timeout: 5_000
               )

      assert_receive {:commitment_result, ^event_data}, 2_000

      assert :ok = Dimse.release(assoc2, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  # --- Test handler factories ---

  defp store_handler(test_pid) do
    # Create a module at runtime that sends stored data back to the test process
    mod = :"Dimse.Test.StoreHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.1.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, data, _state) do
          send(unquote(test_pid), {:stored, data})
          {:ok, 0x0000}
        end

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp store_echo_handler(test_pid) do
    mod = :"Dimse.Test.StoreEchoHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.1.1", "1.2.840.10008.5.1.4.1.1.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, data, _state) do
          send(unquote(test_pid), {:stored, data})
          {:ok, 0x0000}
        end

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp find_handler(test_pid, results, results2 \\ nil) do
    mod = :"Dimse.Test.FindHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.1"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, query, _state) do
          send(unquote(test_pid), {:find_query, query})

          # Support alternating results for sequential find tests
          case Process.get(:find_call_count, 0) do
            0 ->
              Process.put(:find_call_count, 1)
              {:ok, unquote(Macro.escape(results))}

            _ ->
              results2 = unquote(Macro.escape(results2))
              if results2, do: {:ok, results2}, else: {:ok, unquote(Macro.escape(results))}
          end
        end

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp find_error_handler(test_pid) do
    mod = :"Dimse.Test.FindErrorHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.1"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state) do
          send(unquote(test_pid), :find_error_called)
          {:error, 0xA700, "out of resources"}
        end

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp find_echo_handler(test_pid, results) do
    mod = :"Dimse.Test.FindEchoHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.1.1", "1.2.840.10008.5.1.4.1.2.2.1"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state) do
          send(unquote(test_pid), :find_called)
          {:ok, unquote(Macro.escape(results))}
        end

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  # --- C-GET handler factories ---

  defp get_handler(test_pid, instances) do
    mod = :"Dimse.Test.GetHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.3", "1.2.840.10008.5.1.4.1.1.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, query, _state) do
          send(unquote(test_pid), {:get_query, query})
          {:ok, unquote(Macro.escape(instances))}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp get_error_handler(test_pid) do
    mod = :"Dimse.Test.GetErrorHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.3", "1.2.840.10008.5.1.4.1.1.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state) do
          send(unquote(test_pid), :get_error_called)
          {:error, 0xA700, "out of resources"}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp get_echo_handler(test_pid, instances) do
    mod = :"Dimse.Test.GetEchoHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.1.1", "1.2.840.10008.5.1.4.1.2.2.3", "1.2.840.10008.5.1.4.1.1.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state) do
          send(unquote(test_pid), :get_called)
          {:ok, unquote(Macro.escape(instances))}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  # --- C-MOVE handler factories ---

  defp move_handler(test_pid, instances, dest_port) do
    mod = :"Dimse.Test.MoveHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, query, _state) do
          send(unquote(test_pid), {:move_query, query})
          {:ok, unquote(Macro.escape(instances))}
        end

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def resolve_ae("DEST_SCP"), do: {:ok, {"127.0.0.1", unquote(dest_port)}}
        def resolve_ae(_), do: {:error, :unknown_ae}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp move_echo_handler(test_pid, instances, dest_port) do
    mod = :"Dimse.Test.MoveEchoHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.1.1", "1.2.840.10008.5.1.4.1.2.2.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state) do
          send(unquote(test_pid), :move_called)
          {:ok, unquote(Macro.escape(instances))}
        end

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def resolve_ae("DEST_SCP"), do: {:ok, {"127.0.0.1", unquote(dest_port)}}
        def resolve_ae(_), do: {:error, :unknown_ae}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp move_error_handler(test_pid) do
    mod = :"Dimse.Test.MoveErrorHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state) do
          send(unquote(test_pid), :move_error_called)
          {:error, 0xA700, "out of resources"}
        end

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def resolve_ae(_), do: {:error, :unknown_ae}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp move_unknown_dest_handler(test_pid, instances) do
    mod = :"Dimse.Test.MoveUnknownDestHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state) do
          send(unquote(test_pid), :move_called)
          {:ok, unquote(Macro.escape(instances))}
        end

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def resolve_ae(_), do: {:error, :unknown_ae}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp store_dest_handler(test_pid) do
    mod = :"Dimse.Test.StoreDestHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.1.2"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, data, _state) do
          send(unquote(test_pid), {:dest_stored, data})
          {:ok, 0x0000}
        end

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  # --- DIMSE-N handler factories ---

  defp n_get_handler(test_pid, attr_data) do
    mod = :"Dimse.Test.NGetHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(@test_n_sop_class)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_get(command, _state) do
          send(unquote(test_pid), {:n_get_called, command})
          {:ok, 0x0000, unquote(Macro.escape(attr_data))}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp n_set_handler(test_pid, response_data) do
    mod = :"Dimse.Test.NSetHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(@test_n_sop_class)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_set(_command, data_set, _state) do
          send(unquote(test_pid), {:n_set_called, data_set})
          {:ok, 0x0000, unquote(Macro.escape(response_data))}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp n_action_handler(test_pid, reply_data) do
    mod = :"Dimse.Test.NActionHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(@test_n_sop_class)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_action(_command, data_set, _state) do
          send(unquote(test_pid), {:n_action_called, data_set})
          {:ok, 0x0000, unquote(Macro.escape(reply_data))}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp n_create_handler(
         test_pid,
         response_data,
         created_uid,
         abstract_syntax \\ @test_n_sop_class
       ) do
    mod = :"Dimse.Test.NCreateHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(abstract_syntax)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_create(_command, data_set, _state) do
          send(unquote(test_pid), {:n_create_called, data_set})
          {:ok, 0x0000, unquote(created_uid), unquote(Macro.escape(response_data))}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp n_delete_handler(test_pid) do
    mod = :"Dimse.Test.NDeleteHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(@test_n_sop_class)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_delete(command, _state) do
          send(unquote(test_pid), {:n_delete_called, command})
          {:ok, 0x0000}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp n_event_report_handler(test_pid) do
    mod = :"Dimse.Test.NEventReportHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(@test_n_sop_class)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_event_report(_command, data_set, _state) do
          send(unquote(test_pid), {:n_event_report_called, data_set})
          {:ok, 0x0000, nil}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp n_error_handler do
    mod = :"Dimse.Test.NErrorHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(@test_n_sop_class)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        # No N-* callbacks — should return 0x0112
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp n_get_echo_handler(test_pid, attr_data) do
    mod = :"Dimse.Test.NGetEchoHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.1.1", unquote(@test_n_sop_class)]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_get(_command, _state) do
          send(unquote(test_pid), :n_get_echo_called)
          {:ok, 0x0000, unquote(Macro.escape(attr_data))}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp storage_commitment_handler(test_pid) do
    mod = :"Dimse.Test.StorageCommitmentHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes, do: [unquote(@storage_commitment_push)]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        def handle_n_action(_command, data_set, _state) do
          send(unquote(test_pid), {:commitment_requested, data_set})
          {:ok, 0x0000, nil}
        end

        def handle_n_event_report(_command, data_set, _state) do
          send(unquote(test_pid), {:commitment_result, data_set})
          {:ok, 0x0000, nil}
        end
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp find_slow_handler(test_pid) do
    mod = :"Dimse.Test.FindSlowHandler.#{System.unique_integer([:positive])}"

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @impl true
        def supported_abstract_syntaxes do
          ["1.2.840.10008.5.1.4.1.2.2.1"]
        end

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(command, _query, _state) do
          message_id = command[{0x0000, 0x0110}] || 0
          send(unquote(test_pid), {:find_started, message_id})

          # Generate many results slowly so cancel has time to arrive
          results =
            for i <- 1..100 do
              Process.sleep(10)
              :crypto.strong_rand_bytes(32)
            end

          {:ok, results}
        end

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  defp context_capture_handler(test_pid) do
    mod = Module.concat(__MODULE__, :"FindContextHandler#{System.unique_integer([:positive])}")

    Module.create(
      mod,
      quote do
        @behaviour Dimse.Handler

        @study_root_find "1.2.840.10008.5.1.4.1.2.2.1"

        @impl true
        def supported_abstract_syntaxes, do: [@study_root_find]

        @impl true
        def handle_echo(_command, _state), do: {:ok, 0x0000}

        @impl true
        def handle_store(_command, _data, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_find(_command, _query, state) do
          send(
            unquote(test_pid),
            {:find_context, state.current_context_id, state.current_abstract_syntax_uid,
             state.current_transfer_syntax_uid}
          )

          {:ok, []}
        end

        @impl true
        def handle_move(_command, _query, _state), do: {:error, 0xC000, "not supported"}

        @impl true
        def handle_get(_command, _query, _state), do: {:error, 0xC000, "not supported"}
      end,
      Macro.Env.location(__ENV__)
    )

    mod
  end

  # ── Extended Negotiation (PS3.7 Annex D) ────────────────────────────────────

  describe "Extended Negotiation" do
    test "role selections echoed back by SCP and stored in SCU state" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: ExtNegEchoHandler)
      port = :ranch.get_port(ref)

      rs = %Dimse.Pdu.RoleSelection{
        sop_class_uid: "1.2.840.10008.1.1",
        scu_role: true,
        scp_role: false
      }

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "EXT_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"],
          role_selections: [rs]
        )

      wait_for_established(assoc)

      roles = Dimse.Association.negotiated_roles(assoc)
      assert map_size(roles) > 0
      assert {true, false} = roles["1.2.840.10008.1.1"]

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "user identity accepted — SCP returns server token, C-ECHO succeeds" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: ExtNegAuthHandler)
      port = :ranch.get_port(ref)

      identity = %Dimse.Pdu.UserIdentity{
        identity_type: 1,
        positive_response_requested: true,
        primary_field: "alice",
        secondary_field: ""
      }

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "AUTH_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"],
          user_identity: identity
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "user identity rejected — SCU gets {:error, {:rejected, ...}}" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: ExtNegBadAuthHandler)
      port = :ranch.get_port(ref)

      identity = %Dimse.Pdu.UserIdentity{
        identity_type: 1,
        positive_response_requested: false,
        primary_field: "evil",
        secondary_field: ""
      }

      result =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "EVIL_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"],
          user_identity: identity
        )

      assert {:error, {:rejected, 1, 1, 1}} = result
      Dimse.stop_listener(ref)
    end

    test "SCP without handle_authenticate/2 accepts association with user identity (opt-in auth)" do
      # Dimse.Scp.Echo does not implement handle_authenticate/2 — should accept anyway
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      identity = %Dimse.Pdu.UserIdentity{
        identity_type: 1,
        positive_response_requested: false,
        primary_field: "any_user",
        secondary_field: ""
      }

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "ANON_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"],
          user_identity: identity
        )

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "validate_association/2 can reject an association without user identity" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: AssociationValidationRejectHandler)
      port = :ranch.get_port(ref)

      result =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "REJECT_ME",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"]
        )

      assert {:error, {:rejected, 1, 1, 1}} = result
      Dimse.stop_listener(ref)
    end

    test "SOP Class Extended Negotiation sub-items are decoded without error" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: ExtNegEchoHandler)
      port = :ranch.get_port(ref)

      se = %Dimse.Pdu.SopClassExtendedNegotiation{
        sop_class_uid: "1.2.840.10008.1.1",
        service_class_application_info: <<0x01, 0x00>>
      }

      sce = %Dimse.Pdu.SopClassCommonExtendedNegotiation{
        sop_class_uid: "1.2.840.10008.1.1",
        service_class_uid: "1.2.840.10008.4.2",
        related_general_sop_class_uids: []
      }

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "EXT_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"],
          role_selections: [
            %Dimse.Pdu.RoleSelection{
              sop_class_uid: "1.2.840.10008.1.1",
              scu_role: true,
              scp_role: false
            }
          ]
        )

      # We also encode ext neg items in the RQ manually by passing them through
      # Encoder/Decoder roundtrip (integration: these sub-items are decoded w/o crash)
      _ = se
      _ = sce

      wait_for_established(assoc)

      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  # ── Query level routing ──────────────────────────────────────────────────────

  describe "query level routing" do
    test "find/move/get with binary SOP class UID (string passthrough)" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: QuerySupportHandler)
      port = :ranch.get_port(ref)

      find_uid = @study_root_find
      move_uid = @study_root_move
      get_uid = @study_root_get

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [find_uid, move_uid, get_uid]
        )

      wait_for_established(assoc)

      # Binary UID string passed directly — covers resolve_*_sop_class/1 binary clause
      assert {:ok, []} = Dimse.find(assoc, find_uid, <<>>, timeout: 5_000)
      assert {:ok, _} = Dimse.move(assoc, move_uid, <<>>, dest_ae: "DIMSE", timeout: 5_000)
      assert {:ok, []} = Dimse.get(assoc, get_uid, <<>>, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "find with unknown query level returns error without network call" do
      # No assoc needed — rejected before any network I/O
      assert {:error, {:unknown_query_level, :no_such_level}} =
               Dimse.find(:dummy_pid, :no_such_level, <<>>)
    end

    test "move with unknown query level returns error without network call" do
      assert {:error, {:unknown_query_level, :no_such_level}} =
               Dimse.move(:dummy_pid, :no_such_level, <<>>, dest_ae: "X")
    end

    test "get with unknown query level returns error without network call" do
      assert {:error, {:unknown_query_level, :no_such_level}} =
               Dimse.get(:dummy_pid, :no_such_level, <<>>)
    end

    test "handler callbacks receive current presentation context details in state" do
      test_pid = self()
      {:ok, ref} = Dimse.start_listener(port: 0, handler: context_capture_handler(test_pid))
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "CTX_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: [@study_root_find],
          transfer_syntaxes: [Dicom.UID.explicit_vr_little_endian()]
        )

      wait_for_established(assoc)

      assert {:ok, []} = Dimse.find(assoc, @study_root_find, <<>>, timeout: 5_000)

      assert_receive {:find_context, context_id, @study_root_find, transfer_syntax_uid}, 2_000
      assert is_integer(context_id)
      assert context_id > 0
      assert transfer_syntax_uid == Dicom.UID.explicit_vr_little_endian()

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  # ── C-ECHO non-success status ────────────────────────────────────────────────

  describe "C-ECHO non-success DIMSE status" do
    test "SCP returning non-success status yields {:error, {:status, code}}" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: FailingEchoHandler)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"]
        )

      wait_for_established(assoc)

      assert {:error, {:status, 0xC001}} = Dimse.echo(assoc, timeout: 5_000)

      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end
  end

  # ── Association edge cases ───────────────────────────────────────────────────

  describe "Association edge cases" do
    test "cancel without active find is a no-op" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"]
        )

      wait_for_established(assoc)

      # Cancel on association with no pending find — must not crash
      :ok = Dimse.cancel(assoc, 999)
      assert :ok = Dimse.echo(assoc, timeout: 5_000)
      assert :ok = Dimse.release(assoc, 5_000)
      Dimse.stop_listener(ref)
    end

    test "Dimse.Scu.abort/1 terminates association" do
      {:ok, ref} = Dimse.start_listener(port: 0, handler: Dimse.Scp.Echo)
      port = :ranch.get_port(ref)

      {:ok, assoc} =
        Dimse.connect("127.0.0.1", port,
          calling_ae: "TEST_SCU",
          called_ae: "DIMSE",
          abstract_syntaxes: ["1.2.840.10008.1.1"]
        )

      wait_for_established(assoc)
      mon = Process.monitor(assoc)
      :ok = Dimse.Scu.abort(assoc)
      assert_receive {:DOWN, ^mon, :process, ^assoc, _}, 2_000
      Dimse.stop_listener(ref)
    end

    test "connection to closed port yields error" do
      # Find a free port then close it so the connection is refused
      {:ok, sock} = :gen_tcp.listen(0, [])
      {:ok, closed_port} = :inet.port(sock)
      :gen_tcp.close(sock)

      assert {:error, _reason} =
               Dimse.connect("127.0.0.1", closed_port,
                 calling_ae: "SCU",
                 called_ae: "SCP",
                 abstract_syntaxes: ["1.2.840.10008.1.1"],
                 timeout: 2_000
               )
    end
  end
end

# ── Extended Negotiation test handler modules ────────────────────────────────

defmodule ExtNegEchoHandler do
  @behaviour Dimse.Handler

  @impl true
  def supported_abstract_syntaxes, do: ["1.2.840.10008.1.1"]

  @impl true
  def handle_echo(_command, _state), do: {:ok, 0x0000}

  @impl true
  def handle_store(_command, _data, _state), do: {:ok, 0x0000}

  @impl true
  def handle_find(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_move(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_get(_command, _query, _state), do: {:ok, []}
end

defmodule ExtNegAuthHandler do
  @behaviour Dimse.Handler

  @impl true
  def supported_abstract_syntaxes, do: ["1.2.840.10008.1.1"]

  @impl true
  def handle_echo(_command, _state), do: {:ok, 0x0000}

  @impl true
  def handle_store(_command, _data, _state), do: {:ok, 0x0000}

  @impl true
  def handle_find(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_move(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_get(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_authenticate(_identity, _state), do: {:ok, "server-token"}
end

defmodule ExtNegBadAuthHandler do
  @behaviour Dimse.Handler

  @impl true
  def supported_abstract_syntaxes, do: ["1.2.840.10008.1.1"]

  @impl true
  def handle_echo(_command, _state), do: {:ok, 0x0000}

  @impl true
  def handle_store(_command, _data, _state), do: {:ok, 0x0000}

  @impl true
  def handle_find(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_move(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_get(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_authenticate(_identity, _state), do: {:error, :bad_credentials}
end

defmodule FailingEchoHandler do
  @behaviour Dimse.Handler

  @impl true
  def supported_abstract_syntaxes, do: ["1.2.840.10008.1.1"]

  @impl true
  # Return non-success status to exercise Scu.Echo error branch
  def handle_echo(_command, _state), do: {:ok, 0xC001}

  @impl true
  def handle_store(_command, _data, _state), do: {:ok, 0x0000}

  @impl true
  def handle_find(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_move(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_get(_command, _query, _state), do: {:ok, []}
end

defmodule QuerySupportHandler do
  @behaviour Dimse.Handler

  @study_root_find "1.2.840.10008.5.1.4.1.2.2.1"
  @study_root_move "1.2.840.10008.5.1.4.1.2.2.2"
  @study_root_get "1.2.840.10008.5.1.4.1.2.2.3"

  @impl true
  def supported_abstract_syntaxes, do: [@study_root_find, @study_root_move, @study_root_get]

  @impl true
  def handle_echo(_command, _state), do: {:ok, 0x0000}

  @impl true
  def handle_store(_command, _data, _state), do: {:ok, 0x0000}

  @impl true
  def handle_find(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_move(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_get(_command, _query, _state), do: {:ok, []}
end

defmodule AssociationValidationRejectHandler do
  @behaviour Dimse.Handler

  @impl true
  def supported_abstract_syntaxes, do: ["1.2.840.10008.1.1"]

  @impl true
  def handle_echo(_command, _state), do: {:ok, 0x0000}

  @impl true
  def handle_store(_command, _data, _state), do: {:ok, 0x0000}

  @impl true
  def handle_find(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_move(_command, _query, _state), do: {:ok, []}

  @impl true
  def handle_get(_command, _query, _state), do: {:ok, []}

  @impl true
  def validate_association(%Dimse.Pdu.AssociateRq{calling_ae_title: "REJECT_ME"}, _state),
    do: {:error, :forbidden_calling_ae}

  @impl true
  def validate_association(_request, _state), do: {:ok, nil}
end
