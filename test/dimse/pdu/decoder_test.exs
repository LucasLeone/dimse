defmodule Dimse.Pdu.DecoderTest do
  use ExUnit.Case, async: true

  alias Dimse.Pdu
  alias Dimse.Pdu.Decoder
  alias Dimse.Test.PduHelpers

  describe "decode/1 incomplete data" do
    test "returns {:incomplete, data} for empty binary" do
      assert {:incomplete, <<>>} = Decoder.decode(<<>>)
    end

    test "returns {:incomplete, data} for partial header (< 6 bytes)" do
      assert {:incomplete, <<0x01, 0x00>>} = Decoder.decode(<<0x01, 0x00>>)
      assert {:incomplete, <<0x01, 0x00, 0x00>>} = Decoder.decode(<<0x01, 0x00, 0x00>>)
    end

    test "returns {:incomplete, data} when payload is shorter than declared length" do
      # Header says 100 bytes but only 4 present
      data = <<0x01, 0x00, 0x00, 0x00, 0x00, 100, "abcd">>
      assert {:incomplete, ^data} = Decoder.decode(data)
    end
  end

  describe "decode/1 A-RELEASE-RQ" do
    test "decodes valid A-RELEASE-RQ" do
      assert {:ok, %Pdu.ReleaseRq{}, <<>>} = Decoder.decode(PduHelpers.release_rq_binary())
    end

    test "returns remaining bytes" do
      binary = PduHelpers.release_rq_binary() <> <<0xFF>>
      assert {:ok, %Pdu.ReleaseRq{}, <<0xFF>>} = Decoder.decode(binary)
    end
  end

  describe "decode/1 A-RELEASE-RP" do
    test "decodes valid A-RELEASE-RP" do
      assert {:ok, %Pdu.ReleaseRp{}, <<>>} = Decoder.decode(PduHelpers.release_rp_binary())
    end
  end

  describe "decode/1 A-ABORT" do
    test "decodes with source and reason" do
      assert {:ok, %Pdu.Abort{source: 2, reason: 6}, <<>>} =
               Decoder.decode(PduHelpers.abort_binary(2, 6))
    end

    test "decodes service-user abort" do
      assert {:ok, %Pdu.Abort{source: 0, reason: 0}, <<>>} =
               Decoder.decode(PduHelpers.abort_binary(0, 0))
    end

    test "decodes using PduHelpers.abort_binary default args" do
      binary = PduHelpers.abort_binary()
      assert {:ok, %Pdu.Abort{source: 0, reason: 0}, <<>>} = Decoder.decode(binary)
    end
  end

  describe "decode/1 A-ASSOCIATE-RJ" do
    test "decodes rejection with result, source, and reason" do
      assert {:ok, %Pdu.AssociateRj{result: 1, source: 1, reason: 1}, <<>>} =
               Decoder.decode(PduHelpers.associate_rj_binary(1, 1, 1))
    end

    test "decodes transient rejection" do
      assert {:ok, %Pdu.AssociateRj{result: 2, source: 1, reason: 2}, <<>>} =
               Decoder.decode(PduHelpers.associate_rj_binary(2, 1, 2))
    end

    test "decodes using PduHelpers.associate_rj_binary default args" do
      binary = PduHelpers.associate_rj_binary()

      assert {:ok, %Pdu.AssociateRj{result: 1, source: 1, reason: 1}, <<>>} =
               Decoder.decode(binary)
    end
  end

  describe "decode/1 error paths" do
    test "unknown PDU type returns error" do
      binary = <<0xFF, 0x00, 4::32, 0::32>>
      assert {:error, {:unknown_pdu_type, 0xFF}} = Decoder.decode(binary)
    end

    test "non-zero reserved byte returns malformed_pdu" do
      # second byte is 0x01 instead of 0x00 — no clause matches
      binary = <<0x01, 0x01, 0::40>>
      assert {:error, :malformed_pdu} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-RQ with payload too short returns error" do
      binary = <<0x01, 0x00, 5::32, 1, 2, 3, 4, 5>>
      assert {:error, :malformed_associate_rq} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-AC with payload too short returns error" do
      binary = <<0x02, 0x00, 5::32, 1, 2, 3, 4, 5>>
      assert {:error, :malformed_associate_ac} = Decoder.decode(binary)
    end

    test "P-DATA-TF with malformed PDV items returns error" do
      # pdv_length=2 means data_length=0, but payload is 2 bytes (context_id+flags only)
      # feed a length that exceeds the remaining bytes to hit parse_pdv_items catch-all
      binary = <<0x04, 0x00, 2::32, 0, 0>>
      assert {:error, :malformed_pdv} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-RQ with malformed presentation context returns error" do
      # Build valid fixed header (68 bytes) + a 0x20 PC item too short to parse
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      bad_pc_item = <<0x20, 0x00, 3::16, 1, 2, 3>>
      payload = header <> bad_pc_item
      binary = <<0x01, 0x00, byte_size(payload)::32>> <> payload
      assert {:error, :malformed_presentation_context} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-RQ with bad syntax item in presentation context returns error" do
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      # PC: id=1, reserved x3, then a bad item type 0xAA
      pc_content = <<1, 0, 0, 0, 0xAA, 0x00, 1::16, 0x01>>
      pc_item = <<0x20, 0x00, byte_size(pc_content)::16>> <> pc_content
      payload = header <> pc_item
      binary = <<0x01, 0x00, byte_size(payload)::32>> <> payload
      assert {:error, :malformed_syntax_items} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-RQ with unknown variable item type skips it and succeeds" do
      # Valid 68-byte RQ header + unknown item type 0x99 with 0 bytes payload
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      unknown_item = <<0x99, 0x00, 0::16>>
      payload = header <> unknown_item
      binary = <<0x01, 0x00, byte_size(payload)::32>> <> payload
      assert {:ok, %Pdu.AssociateRq{}, <<>>} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-RQ with truncated variable items section returns malformed error" do
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      # 1-byte tail that doesn't match any variable item pattern
      bad_items = <<0xFF>>
      payload = header <> bad_items
      binary = <<0x01, 0x00, byte_size(payload)::32>> <> payload
      assert {:error, :malformed_variable_items} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-RQ with 0x50 user info containing malformed data returns error" do
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      # 0x50 user info with 1 byte that can't match any sub-item pattern
      bad_ui = <<0x50, 0x00, 1::16, 0xFF>>
      payload = header <> bad_ui
      binary = <<0x01, 0x00, byte_size(payload)::32>> <> payload
      assert {:error, :malformed_user_information} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-RQ with 0x50 user info containing unknown sub-item skips it" do
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      # 0x50 user info with unknown sub-item type 0x99, 0 bytes of data
      unknown_sub_item = <<0x99, 0x00, 0::16>>
      ui_item = <<0x50, 0x00, byte_size(unknown_sub_item)::16>> <> unknown_sub_item
      payload = header <> ui_item
      binary = <<0x01, 0x00, byte_size(payload)::32>> <> payload
      assert {:ok, %Pdu.AssociateRq{}, <<>>} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-AC with malformed 0x21 PC item returns error" do
      # Valid AC header (68 bytes) + 0x21 item with only 2 bytes payload (need 4+)
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      bad_pc_item = <<0x21, 0x00, 2::16, 1, 2>>
      payload = header <> bad_pc_item
      binary = <<0x02, 0x00, byte_size(payload)::32>> <> payload
      assert {:error, :malformed_presentation_context} = Decoder.decode(binary)
    end

    test "A-ASSOCIATE-AC with 0x21 PC item containing bad syntax sub-items returns error" do
      header =
        <<1::16, 0::16>> <>
          String.duplicate(" ", 16) <> String.duplicate(" ", 16) <> :binary.copy(<<0>>, 32)

      # 0x21 AC item: valid 4-byte header (id=1, 0x00, result=0, 0x00) + unknown sub-item type
      pc_content = <<1, 0, 0, 0, 0xBB, 0x00, 1::16, 0x01>>
      pc_item = <<0x21, 0x00, byte_size(pc_content)::16>> <> pc_content
      payload = header <> pc_item
      binary = <<0x02, 0x00, byte_size(payload)::32>> <> payload
      assert {:error, :malformed_syntax_items} = Decoder.decode(binary)
    end
  end

  describe "decode/1 A-ASSOCIATE-RQ" do
    test "decodes minimal A-ASSOCIATE-RQ" do
      binary = PduHelpers.associate_rq_binary()
      assert {:ok, %Pdu.AssociateRq{} = rq, <<>>} = Decoder.decode(binary)
      assert rq.protocol_version == 1
      assert rq.called_ae_title == "DIMSE"
      assert rq.calling_ae_title == "TEST_SCU"
    end

    test "decodes presentation contexts" do
      binary = PduHelpers.associate_rq_binary()
      {:ok, rq, <<>>} = Decoder.decode(binary)

      assert [%Pdu.PresentationContext{} = pc] = rq.presentation_contexts
      assert pc.id == 1
      assert pc.abstract_syntax == "1.2.840.10008.1.1"
      assert "1.2.840.10008.1.2" in pc.transfer_syntaxes
      assert "1.2.840.10008.1.2.1" in pc.transfer_syntaxes
    end

    test "decodes presentation context RQ with non-zero reserved bytes" do
      application_context = "1.2.840.10008.3.1.1.1"
      abstract_syntax = "1.2.840.10008.5.1.1.9"
      transfer_syntax = "1.2.840.10008.1.2"

      header =
        <<1::16, 0::16>> <>
          String.pad_trailing("IMPRINTSCP", 16) <>
          String.pad_trailing("TCPPRT", 16) <>
          :binary.copy(<<0>>, 32)

      application_context_item =
        <<0x10, 0x00, byte_size(application_context)::16>> <> application_context

      abstract_syntax_item =
        <<0x30, 0x00, byte_size(abstract_syntax)::16>> <> abstract_syntax

      transfer_syntax_item =
        <<0x40, 0x00, byte_size(transfer_syntax)::16>> <> transfer_syntax

      presentation_context =
        <<1, 0, 0xFF, 0>> <> abstract_syntax_item <> transfer_syntax_item

      presentation_context_item =
        <<0x20, 0x00, byte_size(presentation_context)::16>> <> presentation_context

      payload = header <> application_context_item <> presentation_context_item
      binary = <<0x01, 0x00, byte_size(payload)::32>> <> payload

      assert {:ok, %Pdu.AssociateRq{} = rq, <<>>} = Decoder.decode(binary)
      assert [%Pdu.PresentationContext{} = pc] = rq.presentation_contexts
      assert pc.id == 1
      assert pc.abstract_syntax == abstract_syntax
      assert pc.transfer_syntaxes == [transfer_syntax]
    end

    test "decodes user information" do
      binary = PduHelpers.associate_rq_binary(max_pdu_length: 32_768)
      {:ok, rq, <<>>} = Decoder.decode(binary)

      assert %Pdu.UserInformation{} = rq.user_information
      assert rq.user_information.max_pdu_length == 32_768
      assert rq.user_information.implementation_uid == "1.2.3.4.5"
      assert rq.user_information.implementation_version == "TEST_0.1"
    end
  end

  describe "decode/1 P-DATA-TF" do
    test "decodes a single command PDV" do
      data = <<0xDE, 0xAD>>

      binary =
        PduHelpers.p_data_binary([
          %{context_id: 1, is_command: true, is_last: true, data: data}
        ])

      assert {:ok, %Pdu.PDataTf{pdv_items: [pdv]}, <<>>} = Decoder.decode(binary)
      assert pdv.context_id == 1
      assert pdv.is_command == true
      assert pdv.is_last == true
      assert pdv.data == data
    end

    test "decodes multiple PDV items" do
      binary =
        PduHelpers.p_data_binary([
          %{context_id: 1, is_command: true, is_last: true, data: <<1, 2>>},
          %{context_id: 1, is_command: false, is_last: true, data: <<3, 4>>}
        ])

      assert {:ok, %Pdu.PDataTf{pdv_items: [pdv1, pdv2]}, <<>>} = Decoder.decode(binary)
      assert pdv1.is_command == true
      assert pdv2.is_command == false
    end

    test "decodes flags correctly" do
      # command=false, last=false -> 0x00
      binary =
        PduHelpers.p_data_binary([
          %{context_id: 1, is_command: false, is_last: false, data: <<0>>}
        ])

      {:ok, %Pdu.PDataTf{pdv_items: [pdv]}, _} = Decoder.decode(binary)
      assert pdv.is_command == false
      assert pdv.is_last == false
    end
  end

  describe "PduHelpers utility accessors" do
    test "verification_uid/0 returns Verification SOP class UID" do
      assert PduHelpers.verification_uid() == "1.2.840.10008.1.1"
    end

    test "implicit_vr_le/0 returns Implicit VR Little Endian UID" do
      assert PduHelpers.implicit_vr_le() == "1.2.840.10008.1.2"
    end

    test "explicit_vr_le/0 returns Explicit VR Little Endian UID" do
      assert PduHelpers.explicit_vr_le() == "1.2.840.10008.1.2.1"
    end

    test "pad_ae/1 pads to 16 bytes" do
      assert byte_size(PduHelpers.pad_ae("SCP")) == 16
    end

    test "echo_rq_command/0 returns C-ECHO-RQ map" do
      cmd = PduHelpers.echo_rq_command()
      assert cmd[{0x0000, 0x0100}] == 0x0030
    end

    test "echo_rsp_command/0 returns C-ECHO-RSP map" do
      cmd = PduHelpers.echo_rsp_command()
      assert cmd[{0x0000, 0x0100}] == 0x8030
    end

    test "random_data_set/0 returns 1024 bytes by default" do
      data = PduHelpers.random_data_set()
      assert byte_size(data) == 1024
    end

    test "random_uid/0 generates a dotted numeric UID" do
      uid = PduHelpers.random_uid()
      assert String.starts_with?(uid, "1.2.826.0.1.")
    end

    test "p_data_binary with command=true, last=false produces 0x01 flags" do
      binary =
        PduHelpers.p_data_binary([
          %{context_id: 1, is_command: true, is_last: false, data: <<1>>}
        ])

      assert {:ok, %Pdu.PDataTf{pdv_items: [pdv]}, <<>>} = Decoder.decode(binary)
      assert pdv.is_command == true
      assert pdv.is_last == false
    end
  end

  describe "encode/decode roundtrip" do
    alias Dimse.Pdu.Encoder

    test "A-RELEASE-RQ survives roundtrip" do
      original = %Pdu.ReleaseRq{}
      binary = IO.iodata_to_binary(Encoder.encode(original))
      assert {:ok, %Pdu.ReleaseRq{}, <<>>} = Decoder.decode(binary)
    end

    test "A-RELEASE-RP survives roundtrip" do
      original = %Pdu.ReleaseRp{}
      binary = IO.iodata_to_binary(Encoder.encode(original))
      assert {:ok, %Pdu.ReleaseRp{}, <<>>} = Decoder.decode(binary)
    end

    test "A-ABORT survives roundtrip" do
      original = %Pdu.Abort{source: 2, reason: 4}
      binary = IO.iodata_to_binary(Encoder.encode(original))
      assert {:ok, decoded, <<>>} = Decoder.decode(binary)
      assert decoded.source == 2
      assert decoded.reason == 4
    end

    test "A-ASSOCIATE-RJ survives roundtrip" do
      original = %Pdu.AssociateRj{result: 1, source: 3, reason: 2}
      binary = IO.iodata_to_binary(Encoder.encode(original))
      assert {:ok, decoded, <<>>} = Decoder.decode(binary)
      assert decoded.result == 1
      assert decoded.source == 3
      assert decoded.reason == 2
    end

    test "A-ASSOCIATE-RQ survives roundtrip" do
      original = PduHelpers.build_associate_rq()
      binary = IO.iodata_to_binary(Encoder.encode(original))
      assert {:ok, decoded, <<>>} = Decoder.decode(binary)

      assert decoded.protocol_version == original.protocol_version
      assert decoded.called_ae_title == original.called_ae_title
      assert decoded.calling_ae_title == original.calling_ae_title
      assert length(decoded.presentation_contexts) == length(original.presentation_contexts)

      [pc_orig] = original.presentation_contexts
      [pc_dec] = decoded.presentation_contexts
      assert pc_dec.id == pc_orig.id
      assert pc_dec.abstract_syntax == pc_orig.abstract_syntax
      assert pc_dec.transfer_syntaxes == pc_orig.transfer_syntaxes
    end

    test "A-ASSOCIATE-AC survives roundtrip" do
      original = PduHelpers.build_associate_ac()
      binary = IO.iodata_to_binary(Encoder.encode(original))
      assert {:ok, decoded, <<>>} = Decoder.decode(binary)

      assert decoded.protocol_version == original.protocol_version
      [pc_dec] = decoded.presentation_contexts
      assert pc_dec.id == 1
      assert pc_dec.result == 0
    end

    test "P-DATA-TF survives roundtrip" do
      original = %Pdu.PDataTf{
        pdv_items: [
          %Pdu.PresentationDataValue{
            context_id: 1,
            is_command: true,
            is_last: true,
            data: <<1, 2, 3, 4, 5, 6, 7, 8>>
          }
        ]
      }

      binary = IO.iodata_to_binary(Encoder.encode(original))
      assert {:ok, decoded, <<>>} = Decoder.decode(binary)
      assert [pdv] = decoded.pdv_items
      assert pdv.context_id == 1
      assert pdv.is_command == true
      assert pdv.is_last == true
      assert pdv.data == <<1, 2, 3, 4, 5, 6, 7, 8>>
    end

    test "multiple PDUs in a stream decode sequentially" do
      rq_binary = IO.iodata_to_binary(Encoder.encode(%Pdu.ReleaseRq{}))
      rp_binary = IO.iodata_to_binary(Encoder.encode(%Pdu.ReleaseRp{}))
      stream = rq_binary <> rp_binary

      assert {:ok, %Pdu.ReleaseRq{}, rest} = Decoder.decode(stream)
      assert {:ok, %Pdu.ReleaseRp{}, <<>>} = Decoder.decode(rest)
    end
  end
end
