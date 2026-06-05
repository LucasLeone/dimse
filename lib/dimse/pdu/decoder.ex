defmodule Dimse.Pdu.Decoder do
  @moduledoc """
  Decodes DICOM PDU binaries into `Dimse.Pdu` structs.

  Implements the binary wire format defined in PS3.8 Section 9.3. Each PDU type
  has a fixed header (1 byte type + 1 byte reserved + 4 byte length) followed by
  a type-specific payload.

  Uses Elixir binary pattern matching for direct translation of the PDU format
  tables in the DICOM standard.

  ## Usage

      {:ok, pdu, rest} = Dimse.Pdu.Decoder.decode(binary)
      {:incomplete, binary} = Dimse.Pdu.Decoder.decode(partial_binary)
      {:error, reason} = Dimse.Pdu.Decoder.decode(invalid_binary)

  The decoder handles incomplete reads by returning `{:incomplete, buffer}` so
  the caller can accumulate more data before retrying.
  """

  alias Dimse.Pdu

  @doc """
  Decodes the next PDU from the given binary.

  Returns:
    - `{:ok, pdu_struct, rest}` — successfully decoded PDU with remaining bytes
    - `{:incomplete, binary}` — not enough data, caller should buffer and retry
    - `{:error, reason}` — malformed or unknown PDU type
  """
  @spec decode(binary()) ::
          {:ok, struct(), binary()} | {:incomplete, binary()} | {:error, term()}
  # Need at least 6 bytes for header
  def decode(data) when byte_size(data) < 6, do: {:incomplete, data}

  # Check if we have the full payload
  def decode(<<_type, 0x00, length::32, payload::binary>> = data)
      when byte_size(payload) < length do
    {:incomplete, data}
  end

  # A-ASSOCIATE-RQ (type 0x01) — PS3.8 Section 9.3.2
  def decode(<<0x01, 0x00, length::32, payload::binary-size(length), rest::binary>>) do
    case parse_associate_rq(payload) do
      {:ok, pdu} -> {:ok, pdu, rest}
      {:error, _} = err -> err
    end
  end

  # A-ASSOCIATE-AC (type 0x02) — PS3.8 Section 9.3.3
  def decode(<<0x02, 0x00, length::32, payload::binary-size(length), rest::binary>>) do
    case parse_associate_ac(payload) do
      {:ok, pdu} -> {:ok, pdu, rest}
      {:error, _} = err -> err
    end
  end

  # A-ASSOCIATE-RJ (type 0x03) — PS3.8 Section 9.3.4
  def decode(<<0x03, 0x00, 4::32, 0x00, result, source, reason, rest::binary>>) do
    {:ok, %Pdu.AssociateRj{result: result, source: source, reason: reason}, rest}
  end

  # P-DATA-TF (type 0x04) — PS3.8 Section 9.3.5
  def decode(<<0x04, 0x00, length::32, payload::binary-size(length), rest::binary>>) do
    case parse_pdv_items(payload, []) do
      {:ok, items} -> {:ok, %Pdu.PDataTf{pdv_items: items}, rest}
      {:error, _} = err -> err
    end
  end

  # A-RELEASE-RQ (type 0x05) — PS3.8 Section 9.3.6
  def decode(<<0x05, 0x00, 4::32, _reserved::32, rest::binary>>) do
    {:ok, %Pdu.ReleaseRq{}, rest}
  end

  # A-RELEASE-RP (type 0x06) — PS3.8 Section 9.3.7
  def decode(<<0x06, 0x00, 4::32, _reserved::32, rest::binary>>) do
    {:ok, %Pdu.ReleaseRp{}, rest}
  end

  # A-ABORT (type 0x07) — PS3.8 Section 9.3.8
  def decode(<<0x07, 0x00, 4::32, 0x00, 0x00, source, reason, rest::binary>>) do
    {:ok, %Pdu.Abort{source: source, reason: reason}, rest}
  end

  # Unknown PDU type
  def decode(<<type, 0x00, _length::32, _::binary>>) do
    {:error, {:unknown_pdu_type, type}}
  end

  def decode(<<_::binary>>), do: {:error, :malformed_pdu}

  ## A-ASSOCIATE-RQ parser

  defp parse_associate_rq(
         <<version::16, _reserved::16, called::binary-size(16), calling::binary-size(16),
           _reserved2::binary-size(32), items::binary>>
       ) do
    case parse_variable_items(items) do
      {:ok, parsed} ->
        {:ok,
         %Pdu.AssociateRq{
           protocol_version: version,
           called_ae_title: String.trim(called),
           calling_ae_title: String.trim(calling),
           application_context: parsed[:application_context],
           presentation_contexts: parsed[:presentation_contexts] || [],
           user_information: parsed[:user_information]
         }}

      {:error, _} = err ->
        err
    end
  end

  defp parse_associate_rq(_), do: {:error, :malformed_associate_rq}

  ## A-ASSOCIATE-AC parser

  defp parse_associate_ac(
         <<version::16, _reserved::16, called::binary-size(16), calling::binary-size(16),
           _reserved2::binary-size(32), items::binary>>
       ) do
    case parse_variable_items(items) do
      {:ok, parsed} ->
        {:ok,
         %Pdu.AssociateAc{
           protocol_version: version,
           called_ae_title: String.trim(called),
           calling_ae_title: String.trim(calling),
           application_context: parsed[:application_context],
           presentation_contexts: parsed[:presentation_contexts] || [],
           user_information: parsed[:user_information]
         }}

      {:error, _} = err ->
        err
    end
  end

  defp parse_associate_ac(_), do: {:error, :malformed_associate_ac}

  ## Variable items parser (for A-ASSOCIATE-RQ/AC)

  defp parse_variable_items(data) do
    case parse_variable_items(data, %{presentation_contexts: []}) do
      {:ok, acc} -> {:ok, Map.update!(acc, :presentation_contexts, &Enum.reverse/1)}
      err -> err
    end
  end

  defp parse_variable_items(<<>>, acc), do: {:ok, acc}

  # Application Context Item (0x10)
  defp parse_variable_items(
         <<0x10, 0x00, len::16, uid::binary-size(len), rest::binary>>,
         acc
       ) do
    parse_variable_items(rest, Map.put(acc, :application_context, uid))
  end

  # Presentation Context Item - RQ (0x20)
  defp parse_variable_items(
         <<0x20, 0x00, len::16, item_data::binary-size(len), rest::binary>>,
         acc
       ) do
    case parse_presentation_context_rq(item_data) do
      {:ok, pc} ->
        parse_variable_items(rest, Map.update!(acc, :presentation_contexts, &[pc | &1]))

      {:error, _} = err ->
        err
    end
  end

  # Presentation Context Item - AC (0x21)
  defp parse_variable_items(
         <<0x21, 0x00, len::16, item_data::binary-size(len), rest::binary>>,
         acc
       ) do
    case parse_presentation_context_ac(item_data) do
      {:ok, pc} ->
        parse_variable_items(rest, Map.update!(acc, :presentation_contexts, &[pc | &1]))

      {:error, _} = err ->
        err
    end
  end

  # User Information Item (0x50)
  defp parse_variable_items(
         <<0x50, 0x00, len::16, ui_data::binary-size(len), rest::binary>>,
         acc
       ) do
    case parse_user_information(ui_data) do
      {:ok, ui} -> parse_variable_items(rest, Map.put(acc, :user_information, ui))
      {:error, _} = err -> err
    end
  end

  # Skip unknown items
  defp parse_variable_items(<<_type, 0x00, len::16, _data::binary-size(len), rest::binary>>, acc) do
    parse_variable_items(rest, acc)
  end

  defp parse_variable_items(_, _acc), do: {:error, :malformed_variable_items}

  ## Presentation Context parsers

  defp parse_presentation_context_rq(
         <<id, _reserved1, _reserved2, _reserved3, sub_items::binary>>
       ) do
    case parse_syntax_items(sub_items) do
      {:ok, abstract, transfers} ->
        {:ok,
         %Pdu.PresentationContext{
           id: id,
           abstract_syntax: abstract,
           transfer_syntaxes: transfers
         }}

      {:error, _} = err ->
        err
    end
  end

  defp parse_presentation_context_rq(_), do: {:error, :malformed_presentation_context}

  defp parse_presentation_context_ac(<<id, 0x00, result, 0x00, sub_items::binary>>) do
    case parse_syntax_items(sub_items) do
      {:ok, _abstract, transfers} ->
        {:ok,
         %Pdu.PresentationContext{
           id: id,
           result: result,
           transfer_syntaxes: transfers
         }}

      {:error, _} = err ->
        err
    end
  end

  defp parse_presentation_context_ac(_), do: {:error, :malformed_presentation_context}

  defp parse_syntax_items(data) do
    case parse_syntax_items(data, nil, []) do
      {:ok, abstract, transfers} -> {:ok, abstract, Enum.reverse(transfers)}
      err -> err
    end
  end

  defp parse_syntax_items(<<>>, abstract, transfers), do: {:ok, abstract, transfers}

  # Abstract Syntax (0x30)
  defp parse_syntax_items(
         <<0x30, 0x00, len::16, uid::binary-size(len), rest::binary>>,
         _abstract,
         transfers
       ) do
    parse_syntax_items(rest, uid, transfers)
  end

  # Transfer Syntax (0x40)
  defp parse_syntax_items(
         <<0x40, 0x00, len::16, uid::binary-size(len), rest::binary>>,
         abstract,
         transfers
       ) do
    parse_syntax_items(rest, abstract, [uid | transfers])
  end

  defp parse_syntax_items(_, _, _), do: {:error, :malformed_syntax_items}

  ## User Information parser

  defp parse_user_information(data) do
    case parse_user_info_items(data, %Pdu.UserInformation{}) do
      {:ok, ui} -> {:ok, reverse_ui_list_fields(ui)}
      err -> err
    end
  end

  defp parse_user_info_items(<<>>, ui), do: {:ok, ui}

  # Max Length (0x51)
  defp parse_user_info_items(<<0x51, 0x00, 4::16, length::32, rest::binary>>, ui) do
    parse_user_info_items(rest, %{ui | max_pdu_length: length})
  end

  # Implementation Class UID (0x52)
  defp parse_user_info_items(
         <<0x52, 0x00, len::16, uid::binary-size(len), rest::binary>>,
         ui
       ) do
    parse_user_info_items(rest, %{ui | implementation_uid: uid})
  end

  # Role Selection (0x54) — PS3.7 Annex D.3.3.4
  defp parse_user_info_items(
         <<0x54, 0x00, len::16, data::binary-size(len), rest::binary>>,
         ui
       ) do
    case parse_role_selection(data) do
      {:ok, rs} ->
        parse_user_info_items(rest, %{ui | role_selections: [rs | ui.role_selections || []]})

      {:error, _} = err ->
        err
    end
  end

  # Implementation Version Name (0x55)
  defp parse_user_info_items(
         <<0x55, 0x00, len::16, version::binary-size(len), rest::binary>>,
         ui
       ) do
    parse_user_info_items(rest, %{ui | implementation_version: version})
  end

  # SOP Class Extended Negotiation (0x56) — PS3.7 Annex D.3.3.5
  defp parse_user_info_items(
         <<0x56, 0x00, len::16, data::binary-size(len), rest::binary>>,
         ui
       ) do
    case parse_sop_class_extended(data) do
      {:ok, en} ->
        parse_user_info_items(rest, %{ui | sop_class_extended: [en | ui.sop_class_extended || []]})

      {:error, _} = err ->
        err
    end
  end

  # SOP Class Common Extended Negotiation (0x57) — PS3.7 Annex D.3.3.6
  defp parse_user_info_items(
         <<0x57, 0x00, len::16, data::binary-size(len), rest::binary>>,
         ui
       ) do
    case parse_sop_class_common_extended(data) do
      {:ok, en} ->
        parse_user_info_items(rest, %{
          ui
          | sop_class_common_extended: [en | ui.sop_class_common_extended || []]
        })

      {:error, _} = err ->
        err
    end
  end

  # User Identity (0x58 — RQ) — PS3.7 Annex D.3.3.7
  defp parse_user_info_items(
         <<0x58, 0x00, len::16, data::binary-size(len), rest::binary>>,
         ui
       ) do
    case parse_user_identity(data) do
      {:ok, identity} ->
        parse_user_info_items(rest, %{ui | user_identity: identity})

      {:error, _} = err ->
        err
    end
  end

  # User Identity (0x59 — AC) — PS3.7 Annex D.3.3.8
  defp parse_user_info_items(
         <<0x59, 0x00, len::16, data::binary-size(len), rest::binary>>,
         ui
       ) do
    case parse_user_identity_ac(data) do
      {:ok, identity_ac} ->
        parse_user_info_items(rest, %{ui | user_identity_ac: identity_ac})

      {:error, _} = err ->
        err
    end
  end

  # Skip unknown user info sub-items
  defp parse_user_info_items(<<_type, 0x00, len::16, _data::binary-size(len), rest::binary>>, ui) do
    parse_user_info_items(rest, ui)
  end

  defp parse_user_info_items(_, _), do: {:error, :malformed_user_information}

  ## Extended Negotiation sub-item parsers

  # 0x54 RoleSelection: <<uid_len::16, uid::binary(uid_len), scu::8, scp::8>>
  defp parse_role_selection(<<uid_len::16, uid::binary-size(uid_len), scu, scp>>) do
    {:ok,
     %Pdu.RoleSelection{
       sop_class_uid: uid,
       scu_role: scu == 1,
       scp_role: scp == 1
     }}
  end

  defp parse_role_selection(_), do: {:error, :malformed_role_selection}

  # 0x56 SopClassExtendedNegotiation: <<uid_len::16, uid::binary(uid_len), app_info::binary>>
  defp parse_sop_class_extended(<<uid_len::16, uid::binary-size(uid_len), app_info::binary>>) do
    {:ok,
     %Pdu.SopClassExtendedNegotiation{
       sop_class_uid: uid,
       service_class_application_info: app_info
     }}
  end

  defp parse_sop_class_extended(_), do: {:error, :malformed_sop_class_extended}

  # 0x57 SopClassCommonExtendedNegotiation:
  #   <<uid_len::16, uid::binary(uid_len),
  #     svc_uid_len::16, svc_uid::binary(svc_uid_len),
  #     related_len::16, related_block::binary(related_len)>>
  defp parse_sop_class_common_extended(
         <<uid_len::16, uid::binary-size(uid_len), svc_uid_len::16,
           svc_uid::binary-size(svc_uid_len), related_len::16,
           related_block::binary-size(related_len)>>
       ) do
    case parse_uid_list(related_block, []) do
      {:ok, related} ->
        {:ok,
         %Pdu.SopClassCommonExtendedNegotiation{
           sop_class_uid: uid,
           service_class_uid: svc_uid,
           related_general_sop_class_uids: related
         }}

      {:error, _} = err ->
        err
    end
  end

  defp parse_sop_class_common_extended(_), do: {:error, :malformed_sop_class_common_extended}

  # Tail-recursive parser for a list of <<len::16, uid::binary(len)>> entries
  defp parse_uid_list(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp parse_uid_list(<<len::16, uid::binary-size(len), rest::binary>>, acc) do
    parse_uid_list(rest, [uid | acc])
  end

  defp parse_uid_list(_, _), do: {:error, :malformed_uid_list}

  # 0x58 UserIdentity (RQ):
  #   <<type::8, resp_req::8, pf_len::16, primary::binary(pf_len), sf_len::16, secondary::binary(sf_len)>>
  defp parse_user_identity(
         <<type, resp_req, pf_len::16, primary::binary-size(pf_len), sf_len::16,
           secondary::binary-size(sf_len)>>
       ) do
    {:ok,
     %Pdu.UserIdentity{
       identity_type: type,
       positive_response_requested: resp_req == 1,
       primary_field: primary,
       secondary_field: secondary
     }}
  end

  defp parse_user_identity(_), do: {:error, :malformed_user_identity}

  # 0x59 UserIdentityAc: <<resp_len::16, response::binary(resp_len)>>
  defp parse_user_identity_ac(<<resp_len::16, response::binary-size(resp_len)>>) do
    {:ok, %Pdu.UserIdentityAc{server_response: response}}
  end

  defp parse_user_identity_ac(_), do: {:error, :malformed_user_identity_ac}

  # Reverses list fields that were accumulated in reverse order during parsing
  # Fast path: no list fields were accumulated (common case — plain association)
  defp reverse_ui_list_fields(
         %Pdu.UserInformation{
           role_selections: nil,
           sop_class_extended: nil,
           sop_class_common_extended: nil
         } = ui
       ),
       do: ui

  defp reverse_ui_list_fields(%Pdu.UserInformation{} = ui) do
    %{
      ui
      | role_selections: reverse_or_nil(ui.role_selections),
        sop_class_extended: reverse_or_nil(ui.sop_class_extended),
        sop_class_common_extended: reverse_or_nil(ui.sop_class_common_extended)
    }
  end

  defp reverse_or_nil(nil), do: nil
  defp reverse_or_nil(list), do: Enum.reverse(list)

  ## P-DATA-TF PDV items parser

  defp parse_pdv_items(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp parse_pdv_items(<<pdv_length::32, rest::binary>>, acc)
       when byte_size(rest) >= pdv_length do
    # pdv_length includes context_id (1) + flags (1) + data
    data_length = pdv_length - 2
    <<context_id, flags, data::binary-size(data_length), remaining::binary>> = rest

    pdv = %Pdu.PresentationDataValue{
      context_id: context_id,
      is_command: Bitwise.band(flags, 0x01) != 0,
      is_last: Bitwise.band(flags, 0x02) != 0,
      data: data
    }

    parse_pdv_items(remaining, [pdv | acc])
  end

  defp parse_pdv_items(_, _), do: {:error, :malformed_pdv}
end
