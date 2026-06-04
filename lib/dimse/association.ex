defmodule Dimse.Association do
  @moduledoc """
  GenServer managing a single DICOM association lifecycle.

  Implements the DICOM Upper Layer state machine defined in PS3.8 Section 9.2.
  Each TCP connection spawns one Association process that owns the socket and
  manages the full lifecycle: negotiation, message exchange, and release/abort.

  ## Modes

  - **SCP mode**: Started by `Dimse.ConnectionHandler` when a TCP connection is
    accepted. Waits for A-ASSOCIATE-RQ, negotiates, then handles DIMSE commands.
  - **SCU mode**: Started by `Dimse.Scu.open/3` to connect to a remote SCP.
    Sends A-ASSOCIATE-RQ, waits for AC, then executes DIMSE operations.
  """

  use GenServer

  alias Dimse.{Pdu, Command, Message, Telemetry, Tls}
  alias Dimse.Pdu.{Encoder, Decoder}
  alias Dimse.Association.{State, Config, Negotiation}
  alias Dimse.Command.Fields

  @implementation_uid "1.2.826.0.1.3680043.8.498.1"
  @implementation_version "DIMSE_0.8.4"

  @default_transfer_syntaxes MapSet.new([
                               "1.2.840.10008.1.2",
                               "1.2.840.10008.1.2.1"
                             ])

  # --- Public API ---

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc false
  def start(opts) do
    GenServer.start(__MODULE__, opts)
  end

  @doc """
  Sends a DIMSE command on the association and waits for the response.

  Used by SCU modules (e.g., `Dimse.Scu.Echo`) to execute operations.
  """
  @spec request(pid(), map(), binary() | nil, timeout()) ::
          {:ok, map(), binary() | nil} | {:error, term()}
  def request(pid, command_set, data \\ nil, timeout \\ 30_000) do
    GenServer.call(pid, {:dimse_request, command_set, data}, timeout)
  end

  @doc """
  Sends a DIMSE command that expects multiple responses (C-FIND).

  Accumulates all Pending responses and returns the collected data sets
  when the final Success/Error response arrives.

  Returns `{:ok, final_command, [binary()]}` or `{:error, term()}`.
  """
  @spec find_request(pid(), map(), binary(), timeout()) ::
          {:ok, map(), [binary()]} | {:error, term()}
  def find_request(pid, command_set, data, timeout \\ 30_000) do
    GenServer.call(pid, {:dimse_find_request, command_set, data}, timeout)
  end

  @doc """
  Sends a DIMSE command that expects multiple responses with interleaved
  C-STORE sub-operations (C-GET).

  The SCU enters `get_mode`, which auto-accepts incoming C-STORE-RQ messages
  (sent by the SCP as sub-operations) and accumulates their data sets. The
  final C-GET-RSP ends the retrieval.

  Returns `{:ok, final_command, [binary()]}` or `{:error, term()}`.
  """
  @spec get_request(pid(), map(), binary(), timeout()) ::
          {:ok, map(), [binary()]} | {:error, term()}
  def get_request(pid, command_set, data, timeout \\ 30_000) do
    GenServer.call(pid, {:dimse_get_request, command_set, data}, timeout)
  end

  @doc """
  Sends a C-CANCEL-RQ to cancel a pending C-FIND operation.
  """
  @spec cancel(pid(), integer()) :: :ok
  def cancel(pid, message_id) do
    GenServer.cast(pid, {:cancel_find, message_id})
  end

  @doc """
  Sends an A-RELEASE-RQ and waits for A-RELEASE-RP.
  """
  @spec release(pid(), timeout()) :: :ok | {:error, term()}
  def release(pid, timeout \\ 30_000) do
    GenServer.call(pid, :release, timeout)
  end

  @doc """
  Sends an A-ABORT and terminates the association.
  """
  @spec abort(pid()) :: :ok
  def abort(pid) do
    GenServer.cast(pid, :abort)
  end

  @doc """
  Returns the negotiated contexts for this association.
  """
  @spec negotiated_contexts(pid()) :: %{pos_integer() => {String.t(), String.t()}}
  def negotiated_contexts(pid) do
    GenServer.call(pid, :get_negotiated_contexts)
  end

  @doc """
  Returns the negotiated role selections for this association.

  Keys are SOP Class UIDs; values are `{scu_role, scp_role}` boolean tuples.
  Returns an empty map when no role selections were negotiated.
  """
  @spec negotiated_roles(pid()) :: %{String.t() => {boolean(), boolean()}}
  def negotiated_roles(pid) do
    GenServer.call(pid, :get_negotiated_roles)
  end

  @doc false
  def test_close_socket(state), do: close_socket(state)

  @doc false
  def test_handler_abstract_syntaxes(handler), do: handler_abstract_syntaxes(handler)

  @doc false
  def test_request_on_negotiated_context?(message, state),
    do: request_on_negotiated_context?(message, state)

  @doc false
  def test_callback_state_for_message(state, message),
    do: callback_state_for_message(state, message)

  @doc false
  def test_get_in_user_info(rq, field), do: get_in_user_info(rq, field)

  @doc false
  def test_normalize_n_dispatch_result(callback, result, command),
    do: normalize_n_dispatch_result(callback, result, command)

  @doc false
  def test_authenticate_user(user_info, handler, state),
    do: authenticate_user(user_info, handler, state)

  @doc false
  def test_validate_association_request(rq, handler, state),
    do: validate_association_request(rq, handler, state)

  @doc false
  def test_echo_role_selections(user_info, accepted),
    do: echo_role_selections(user_info, accepted)

  @doc false
  def test_roles_to_map(roles), do: roles_to_map(roles)

  @doc false
  def test_maybe_put_instance_uid(cmd, command_field, request_command),
    do: maybe_put_instance_uid(cmd, command_field, request_command)

  @doc false
  def test_do_wait_sub_assoc(assoc, deadline), do: do_wait_sub_assoc(assoc, deadline)

  # --- GenServer callbacks ---

  @impl true
  def init(opts) do
    config = Keyword.get(opts, :config, %Config{})
    mode = Keyword.get(opts, :mode, :scp)

    state = %State{
      phase: :idle,
      local_ae_title: Keyword.get(opts, :ae_title, config.ae_title),
      handler: Keyword.get(opts, :handler),
      config: config,
      association_id: generate_id(),
      started_at: System.monotonic_time(:millisecond)
    }

    case mode do
      :scp -> init_scp(state, opts)
      :scu -> init_scu(state, opts)
    end
  end

  defp init_scp(state, opts) do
    ranch_ref = Keyword.get(opts, :ranch_ref)
    transport = Keyword.get(opts, :transport, :ranch_tcp)

    if ranch_ref do
      # Cannot call :ranch.handshake in init — it deadlocks because Ranch
      # waits for start_link to return before sending the handshake message.
      # Use handle_continue to defer the handshake.
      Process.put(:ranch_ref, ranch_ref)
      new_state = %{state | transport: transport, phase: :idle}
      {:ok, new_state, {:continue, :handshake}}
    else
      {:ok, state}
    end
  end

  @impl true
  def handle_continue(:handshake, state) do
    ranch_ref = Process.delete(:ranch_ref)
    {:ok, socket} = :ranch.handshake(ranch_ref)
    state.transport.setopts(socket, active: :once, packet: :raw, mode: :binary)

    new_state =
      %{state | socket: socket}
      |> start_artim_timer()

    Telemetry.emit(:association_start, %{system_time: System.system_time()}, %{
      association_id: state.association_id,
      mode: :scp
    })

    {:noreply, new_state}
  end

  defp init_scu(state, opts) do
    host = Keyword.get(opts, :host)
    port = Keyword.get(opts, :port)
    called_ae = Keyword.get(opts, :called_ae)
    calling_ae = Keyword.get(opts, :calling_ae, state.local_ae_title)
    abstract_syntaxes = Keyword.get(opts, :abstract_syntaxes, [])
    tls_opts = Keyword.get(opts, :tls)
    role_selections = Keyword.get(opts, :role_selections)
    user_identity = Keyword.get(opts, :user_identity)

    transfer_syntaxes =
      Keyword.get(opts, :transfer_syntaxes, MapSet.to_list(@default_transfer_syntaxes))

    timeout = Keyword.get(opts, :timeout, state.config.dimse_timeout)

    {connect_mod, transport, extra_opts} =
      case tls_opts do
        nil ->
          {:gen_tcp, :gen_tcp, []}

        tls when is_list(tls) ->
          ssl_opts = Tls.normalize_opts(tls)
          {:ssl, :ssl, ssl_opts}
      end

    socket_opts = [:binary, active: :once, packet: :raw] ++ extra_opts

    case connect_mod.connect(to_charlist(host), port, socket_opts, timeout) do
      {:ok, socket} ->
        proposed_contexts = proposed_contexts(abstract_syntaxes)

        new_state = %{
          state
          | socket: socket,
            transport: transport,
            local_ae_title: calling_ae,
            remote_ae_title: called_ae,
            proposed_contexts: proposed_contexts,
            phase: :negotiating
        }

        # Build and send A-ASSOCIATE-RQ
        rq =
          build_associate_rq(
            calling_ae,
            called_ae,
            abstract_syntaxes,
            transfer_syntaxes,
            state.config,
            role_selections: role_selections,
            user_identity: user_identity
          )

        send_pdu(new_state, rq)

        Telemetry.emit(:association_start, %{system_time: System.system_time()}, %{
          association_id: state.association_id,
          mode: :scu
        })

        Telemetry.emit_event([:negotiation, :start], %{system_time: System.system_time()}, %{
          association_id: state.association_id,
          mode: :scu,
          calling_ae: calling_ae,
          called_ae: called_ae,
          proposed_contexts_count: length(abstract_syntaxes)
        })

        # Emit TLS handshake event if connected over SSL
        if transport == :ssl, do: emit_tls_handshake(socket, state.association_id)

        {:ok, new_state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:dimse_request, command_set, data}, from, %{phase: :established} = state) do
    send_dimse_request(command_set, data, state, fn ->
      {:noreply, %{state | pending_request: from}}
    end)
  end

  def handle_call(
        {:dimse_find_request, command_set, data},
        from,
        %{phase: :established} = state
      ) do
    send_dimse_request(command_set, data, state, fn ->
      {:noreply, %{state | pending_request: from, collecting_results: true, pending_results: []}}
    end)
  end

  def handle_call(
        {:dimse_get_request, command_set, data},
        from,
        %{phase: :established} = state
      ) do
    send_dimse_request(command_set, data, state, fn ->
      {:noreply,
       %{
         state
         | pending_request: from,
           collecting_results: true,
           pending_results: [],
           get_mode: true
       }}
    end)
  end

  def handle_call(:release, from, %{phase: :established} = state) do
    send_pdu(state, %Pdu.ReleaseRq{})
    start_artim_timer(state)
    {:noreply, %{state | phase: :releasing, pending_release: from}}
  end

  def handle_call(:get_negotiated_contexts, _from, state) do
    {:reply, state.negotiated_contexts, state}
  end

  def handle_call(:get_negotiated_roles, _from, state) do
    {:reply, state.role_selections, state}
  end

  def handle_call(_msg, _from, state), do: {:reply, {:error, :not_established}, state}

  @impl true
  def handle_cast(:abort, state) do
    if state.socket, do: send_pdu(state, %Pdu.Abort{source: 0, reason: 0})
    close_connection(state, :aborted)
  end

  def handle_cast({:cancel_find, message_id}, %{phase: :established} = state) do
    # Build C-CANCEL-RQ command set (PS3.7 Section 9.3.2.3)
    cancel_command = %{
      {0x0000, 0x0100} => Fields.c_cancel_rq(),
      {0x0000, 0x0120} => message_id,
      {0x0000, 0x0800} => 0x0101
    }

    # Use first available negotiated context, fall back to 1
    context_id =
      state.negotiated_contexts
      |> Map.keys()
      |> List.first(1)

    pdus = Message.fragment(cancel_command, nil, context_id, state.max_pdu_length)
    Enum.each(pdus, &send_pdu(state, &1))

    {:noreply, state}
  end

  def handle_cast({:cancel_find, _message_id}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({proto, socket, data}, %{socket: socket} = state)
      when proto in [:tcp, :ssl] do
    # Skip concatenation when buffer is empty (common case: one PDU per TCP segment)
    buffer =
      case state.pdu_buffer do
        "" -> data
        prev -> prev <> data
      end

    new_state = %{state | bytes_received: state.bytes_received + byte_size(data)}

    case process_buffer(buffer, new_state) do
      {:ok, remaining_buffer, final_state} ->
        reactivate_socket(final_state)
        {:noreply, %{final_state | pdu_buffer: remaining_buffer}}

      {:stop, reason, final_state} ->
        close_connection(final_state, reason)
    end
  end

  def handle_info({closed, socket}, %{socket: socket} = state)
      when closed in [:tcp_closed, :ssl_closed] do
    close_connection(state, :tcp_closed)
  end

  def handle_info({error, socket, reason}, %{socket: socket} = state)
      when error in [:tcp_error, :ssl_error] do
    close_connection(state, {:tcp_error, reason})
  end

  def handle_info(:artim_timeout, state) do
    send_pdu(state, %Pdu.Abort{source: 2, reason: 0})
    close_connection(state, :artim_timeout)
  end

  # Sub-operation processing for C-GET and C-MOVE SCP
  def handle_info({:sub_operation, :next}, %{sub_operation: nil} = state) do
    {:noreply, state}
  end

  def handle_info({:sub_operation, :next}, %{sub_operation: sub_op} = state) do
    case sub_op.remaining do
      [] ->
        # All sub-operations complete -- send final response
        send_sub_op_final_response(sub_op, state)

        Telemetry.emit_event([:sub_operation, :stop], %{}, %{
          association_id: state.association_id,
          type: sub_op.type,
          completed: sub_op.completed,
          failed: sub_op.failed,
          warning: sub_op.warning
        })

        # Clean up sub-association for C-MOVE
        if sub_op.sub_assoc, do: Dimse.Scu.release(sub_op.sub_assoc, 5_000)

        {:noreply, %{state | sub_operation: nil}}

      [{sop_class, sop_instance, data} | rest] ->
        updated_sub_op = %{sub_op | remaining: rest}

        case sub_op.type do
          :c_get ->
            # Send C-STORE-RQ on the same association
            store_message_id = System.unique_integer([:positive]) |> Bitwise.band(0xFFFF)

            store_command = %{
              {0x0000, 0x0002} => sop_class,
              {0x0000, 0x0100} => Fields.c_store_rq(),
              {0x0000, 0x0110} => store_message_id,
              {0x0000, 0x0700} => 0x0000,
              {0x0000, 0x0800} => 0x0000,
              {0x0000, 0x1000} => sop_instance
            }

            context_id = find_context_id(state.negotiated_contexts, sop_class)

            if context_id do
              pdus = Message.fragment(store_command, data, context_id, state.max_pdu_length)
              Enum.each(pdus, &send_pdu(state, &1))
              # Wait for C-STORE-RSP -- it will arrive via handle_response
              {:noreply, %{state | sub_operation: updated_sub_op}}
            else
              # No accepted context for this SOP class -- count as failed
              failed_sub_op = %{updated_sub_op | failed: updated_sub_op.failed + 1}
              emit_sub_op_progress(failed_sub_op, state)
              send_sub_op_pending_response(failed_sub_op, state)
              Process.send(self(), {:sub_operation, :next}, [])
              {:noreply, %{state | sub_operation: failed_sub_op}}
            end

          :c_move ->
            # Send C-STORE via outbound sub-association
            case Dimse.Scu.Store.send(
                   sub_op.sub_assoc,
                   sop_class,
                   sop_instance,
                   data,
                   move_originator_ae: state.remote_ae_title,
                   move_originator_message_id: sub_op.message_id,
                   timeout: 30_000
                 ) do
              :ok ->
                completed_sub_op = %{updated_sub_op | completed: updated_sub_op.completed + 1}
                emit_sub_op_progress(completed_sub_op, state)
                send_sub_op_pending_response(completed_sub_op, state)
                Process.send(self(), {:sub_operation, :next}, [])
                {:noreply, %{state | sub_operation: completed_sub_op}}

              {:error, _reason} ->
                failed_sub_op = %{updated_sub_op | failed: updated_sub_op.failed + 1}
                emit_sub_op_progress(failed_sub_op, state)
                send_sub_op_pending_response(failed_sub_op, state)
                Process.send(self(), {:sub_operation, :next}, [])
                {:noreply, %{state | sub_operation: failed_sub_op}}
            end
        end
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state) do
    duration = System.monotonic_time(:millisecond) - state.started_at

    Telemetry.emit(
      :association_stop,
      %{
        duration: duration,
        bytes_received: state.bytes_received,
        bytes_sent: state.bytes_sent
      },
      %{
        association_id: state.association_id,
        reason: reason
      }
    )

    if state.socket do
      close_socket(state)
    end
  end

  # --- Buffer processing ---

  defp process_buffer(buffer, state) do
    case Decoder.decode(buffer) do
      {:ok, pdu, rest} ->
        case handle_pdu(pdu, state) do
          {:ok, new_state} -> process_buffer(rest, new_state)
          {:stop, reason, new_state} -> {:stop, reason, new_state}
        end

      {:incomplete, _} ->
        {:ok, buffer, state}

      {:error, reason} ->
        {:stop, {:decode_error, reason}, state}
    end
  end

  # --- PDU dispatch by state ---

  # SCP Idle: expecting A-ASSOCIATE-RQ
  defp handle_pdu(%Pdu.AssociateRq{} = rq, %{phase: :idle} = state) do
    cancel_artim_timer(state)
    handle_associate_rq(rq, state)
  end

  # SCU Negotiating: expecting A-ASSOCIATE-AC or A-ASSOCIATE-RJ
  defp handle_pdu(%Pdu.AssociateAc{} = ac, %{phase: :negotiating} = state) do
    handle_associate_ac(ac, state)
  end

  defp handle_pdu(%Pdu.AssociateRj{} = rj, %{phase: :negotiating} = state) do
    proposed_count = map_size(state.proposed_contexts)

    Telemetry.emit_event([:negotiation, :stop], %{duration: 0}, %{
      association_id: state.association_id,
      mode: :scu,
      accepted_contexts_count: 0,
      rejected_contexts_count: proposed_count,
      result: :rejected
    })

    if state.pending_request do
      # SCU init is waiting -- we use the init caller stored elsewhere
    end

    {:stop, {:rejected, rj.result, rj.source, rj.reason}, state}
  end

  # Established: P-DATA-TF
  defp handle_pdu(%Pdu.PDataTf{} = pdu, %{phase: :established} = state) do
    handle_p_data(pdu, state)
  end

  # Release
  defp handle_pdu(%Pdu.ReleaseRq{}, %{phase: :established} = state) do
    send_pdu(state, %Pdu.ReleaseRp{})
    {:stop, :normal, %{state | phase: :closed}}
  end

  defp handle_pdu(%Pdu.ReleaseRp{}, %{phase: :releasing} = state) do
    cancel_artim_timer(state)

    if state.pending_release do
      GenServer.reply(state.pending_release, :ok)
    end

    {:stop, :normal, %{state | phase: :closed, pending_release: nil}}
  end

  # Abort from any state
  defp handle_pdu(%Pdu.Abort{} = abort, state) do
    if state.pending_request do
      GenServer.reply(state.pending_request, {:error, {:aborted, abort.source, abort.reason}})
    end

    if state.pending_release do
      GenServer.reply(state.pending_release, {:error, :aborted})
    end

    {:stop, {:aborted, abort.source, abort.reason},
     %{state | pending_request: nil, pending_release: nil}}
  end

  # Unexpected PDU
  defp handle_pdu(_pdu, state) do
    send_pdu(state, %Pdu.Abort{source: 2, reason: 2})
    {:stop, :unexpected_pdu, state}
  end

  # --- Association negotiation (SCP) ---

  defp handle_associate_rq(rq, state) do
    negotiation_start = System.monotonic_time(:millisecond)
    proposed_count = length(rq.presentation_contexts)

    Telemetry.emit_event([:negotiation, :start], %{system_time: System.system_time()}, %{
      association_id: state.association_id,
      mode: :scp,
      calling_ae: rq.calling_ae_title,
      called_ae: rq.called_ae_title,
      proposed_contexts_count: proposed_count
    })

    handler = state.handler
    supported_as = handler_abstract_syntaxes(handler)
    supported_ts = @default_transfer_syntaxes

    {result_contexts, accepted_map} =
      Negotiation.negotiate(rq.presentation_contexts, supported_as, supported_ts)

    rejected_count = proposed_count - map_size(accepted_map)

    if map_size(accepted_map) == 0 do
      # Reject -- no acceptable contexts
      negotiation_duration = System.monotonic_time(:millisecond) - negotiation_start

      Telemetry.emit_event([:negotiation, :stop], %{duration: negotiation_duration}, %{
        association_id: state.association_id,
        mode: :scp,
        accepted_contexts_count: 0,
        rejected_contexts_count: rejected_count,
        result: :rejected
      })

      send_pdu(state, %Pdu.AssociateRj{result: 1, source: 1, reason: 1})
      {:stop, :no_accepted_contexts, state}
    else
      case validate_association_request(rq, handler, state) do
        :ok ->
          case authenticate_user(rq.user_information, handler, state) do
            {:ok, user_identity_ac} ->
              remote_max_pdu =
                case rq.user_information do
                  %Pdu.UserInformation{max_pdu_length: len} when is_integer(len) and len > 0 ->
                    len

                  _ ->
                    16_384
                end

              effective_max_pdu = min(remote_max_pdu, state.config.max_pdu_length)
              role_selections = echo_role_selections(rq.user_information, accepted_map)

              ac = %Pdu.AssociateAc{
                protocol_version: 1,
                called_ae_title: rq.called_ae_title,
                calling_ae_title: state.local_ae_title,
                presentation_contexts: result_contexts,
                user_information: %Pdu.UserInformation{
                  max_pdu_length: state.config.max_pdu_length,
                  implementation_uid: @implementation_uid,
                  implementation_version: @implementation_version,
                  role_selections: role_selections,
                  user_identity_ac: user_identity_ac
                }
              }

              send_pdu(state, ac)

              negotiation_duration = System.monotonic_time(:millisecond) - negotiation_start

              Telemetry.emit_event([:negotiation, :stop], %{duration: negotiation_duration}, %{
                association_id: state.association_id,
                mode: :scp,
                accepted_contexts_count: map_size(accepted_map),
                rejected_contexts_count: rejected_count,
                result: :accepted
              })

              # Emit TLS handshake event if connected over SSL
              if state.transport == :ranch_ssl,
                do: emit_tls_handshake(state.socket, state.association_id)

              {:ok,
               %{
                 state
                 | phase: :established,
                   remote_ae_title: rq.calling_ae_title,
                   negotiated_contexts: accepted_map,
                   role_selections: roles_to_map(role_selections),
                   max_pdu_length: effective_max_pdu,
                   implementation_uid: get_in_user_info(rq, :implementation_uid),
                   implementation_version: get_in_user_info(rq, :implementation_version)
               }}

            {:error, _reason} ->
              negotiation_duration = System.monotonic_time(:millisecond) - negotiation_start

              Telemetry.emit_event([:negotiation, :stop], %{duration: negotiation_duration}, %{
                association_id: state.association_id,
                mode: :scp,
                accepted_contexts_count: 0,
                rejected_contexts_count: proposed_count,
                result: :rejected
              })

              send_pdu(state, %Pdu.AssociateRj{result: 1, source: 1, reason: 1})
              {:stop, :authentication_failed, state}
          end

        {:error, _reason} ->
          negotiation_duration = System.monotonic_time(:millisecond) - negotiation_start

          Telemetry.emit_event([:negotiation, :stop], %{duration: negotiation_duration}, %{
            association_id: state.association_id,
            mode: :scp,
            accepted_contexts_count: 0,
            rejected_contexts_count: proposed_count,
            result: :rejected
          })

          send_pdu(state, %Pdu.AssociateRj{result: 1, source: 1, reason: 1})
          {:stop, :association_rejected, state}
      end
    end
  end

  # --- Association negotiation (SCU) ---

  defp handle_associate_ac(ac, state) do
    accepted =
      for %Pdu.PresentationContext{id: id, result: 0, transfer_syntaxes: [ts | _]} <-
            ac.presentation_contexts,
          sop_class_uid = Map.get(state.proposed_contexts, id),
          not is_nil(sop_class_uid),
          into: %{} do
        {id, {sop_class_uid, ts}}
      end

    proposed_count = map_size(state.proposed_contexts)
    accepted_count = map_size(accepted)

    Telemetry.emit_event([:negotiation, :stop], %{duration: 0}, %{
      association_id: state.association_id,
      mode: :scu,
      accepted_contexts_count: accepted_count,
      rejected_contexts_count: proposed_count - accepted_count,
      result: if(accepted_count > 0, do: :accepted, else: :rejected)
    })

    remote_max_pdu =
      case ac.user_information do
        %Pdu.UserInformation{max_pdu_length: len} when is_integer(len) and len > 0 -> len
        _ -> 16_384
      end

    effective_max_pdu = min(remote_max_pdu, state.config.max_pdu_length)

    negotiated_roles =
      case ac.user_information do
        %Pdu.UserInformation{role_selections: roles} -> roles_to_map(roles)
        _ -> %{}
      end

    {:ok,
     %{
       state
       | phase: :established,
         negotiated_contexts: accepted,
         role_selections: negotiated_roles,
         max_pdu_length: effective_max_pdu,
         implementation_uid: get_in_user_info(ac, :implementation_uid),
         implementation_version: get_in_user_info(ac, :implementation_version)
     }}
  end

  # --- P-DATA handling ---

  defp handle_p_data(%Pdu.PDataTf{pdv_items: items}, state) do
    process_pdv_items(items, state)
  end

  defp process_pdv_items([], state), do: {:ok, state}

  defp process_pdv_items([pdv | rest], state) do
    assembler = state.current_dimse_message || Message.Assembler.new()

    case Message.Assembler.feed(assembler, pdv) do
      {:continue, new_assembler} ->
        process_pdv_items(rest, %{state | current_dimse_message: new_assembler})

      {:complete, message} ->
        new_state = %{state | current_dimse_message: nil}
        handle_dimse_message(message, new_state, rest)

      {:error, reason} ->
        {:stop, {:message_assembly_error, reason}, state}
    end
  end

  defp handle_dimse_message(message, state, remaining_pdvs) do
    command_field = Command.command_field(message.command)

    cond do
      # C-STORE-RQ received while in get_mode (SCU receiving C-STORE sub-ops during C-GET)
      state.get_mode && command_field == Fields.c_store_rq() ->
        handle_get_mode_store(message, state, remaining_pdvs)

      Fields.response?(command_field) ->
        handle_response(message, state, remaining_pdvs)

      Fields.request?(command_field) and request_on_negotiated_context?(message, state) ->
        dispatch_scp_request(message, state, remaining_pdvs)

      Fields.request?(command_field) ->
        send_pdu(state, %Pdu.Abort{source: 2, reason: 6})
        {:stop, :invalid_presentation_context, state}

      true ->
        {:stop, :unexpected_command, state}
    end
  end

  defp callback_state_for_message(state, %Message{context_id: context_id}) do
    case Map.get(state.negotiated_contexts, context_id) do
      {abstract_syntax_uid, transfer_syntax_uid} ->
        %{
          state
          | current_context_id: context_id,
            current_abstract_syntax_uid: abstract_syntax_uid,
            current_transfer_syntax_uid: transfer_syntax_uid
        }

      nil ->
        %{state | current_context_id: context_id}
    end
  end

  defp handle_response(
         _message,
         %{pending_request: nil, sub_operation: nil} = state,
         remaining_pdvs
       ) do
    # Late response with no pending request -- ignore (e.g., after C-CANCEL)
    process_pdv_items(remaining_pdvs, state)
  end

  defp handle_response(
         message,
         %{sub_operation: %{type: :c_get} = sub_op} = state,
         remaining_pdvs
       ) do
    # C-STORE-RSP during C-GET SCP sub-operations
    command_field = Command.command_field(message.command)

    if command_field == Fields.c_store_rsp() do
      status = Command.status(message.command)

      updated_sub_op =
        case Command.Status.category(status) do
          :success -> %{sub_op | completed: sub_op.completed + 1}
          :warning -> %{sub_op | warning: sub_op.warning + 1}
          _ -> %{sub_op | failed: sub_op.failed + 1}
        end

      # Send C-GET-RSP Pending with updated sub-op counts
      emit_sub_op_progress(updated_sub_op, state)
      send_sub_op_pending_response(updated_sub_op, state)

      # Trigger next sub-operation
      Process.send(self(), {:sub_operation, :next}, [])
      process_pdv_items(remaining_pdvs, %{state | sub_operation: updated_sub_op})
    else
      # Not a C-STORE-RSP — unexpected during sub-operation, ignore
      process_pdv_items(remaining_pdvs, state)
    end
  end

  defp handle_response(message, %{collecting_results: true} = state, remaining_pdvs) do
    # Multi-response collection (C-FIND, C-MOVE SCU)
    handle_multi_response(message, state, remaining_pdvs)
  end

  defp handle_response(message, state, remaining_pdvs) do
    # Single response to our SCU request
    GenServer.reply(state.pending_request, {:ok, message.command, message.data})
    process_pdv_items(remaining_pdvs, %{state | pending_request: nil})
  end

  defp handle_multi_response(message, state, remaining_pdvs) do
    status = Command.status(message.command)

    case Command.Status.category(status) do
      :pending ->
        # Accumulate the matching data set
        new_results =
          if message.data,
            do: [message.data | state.pending_results],
            else: state.pending_results

        process_pdv_items(remaining_pdvs, %{state | pending_results: new_results})

      _ ->
        # Final response (success, cancel, or failure)
        results = Enum.reverse(state.pending_results)
        GenServer.reply(state.pending_request, {:ok, message.command, results})

        new_state = %{
          state
          | pending_request: nil,
            collecting_results: false,
            pending_results: [],
            get_mode: false
        }

        process_pdv_items(remaining_pdvs, new_state)
    end
  end

  defp dispatch_scp_request(message, state, remaining_pdvs) do
    handler = state.handler
    command_field = Command.command_field(message.command)
    message_id = Command.message_id(message.command) || 0
    start_time = System.monotonic_time(:millisecond)

    Telemetry.emit(:command_start, %{system_time: System.system_time()}, %{
      association_id: state.association_id,
      command_field: command_field,
      message_id: message_id
    })

    result =
      case command_field do
        0x0030 ->
          # C-ECHO-RQ
          callback_state = callback_state_for_message(state, message)

          invoke_handler(:handle_echo, command_field, state, fn ->
            case handler.handle_echo(message.command, callback_state) do
              {:ok, status} -> {status, nil, %{}}
              {:error, status, _msg} -> {status, nil, %{}}
            end
          end)

        0x0001 ->
          # C-STORE-RQ
          callback_state = callback_state_for_message(state, message)

          invoke_handler(:handle_store, command_field, state, fn ->
            case handler.handle_store(message.command, message.data, callback_state) do
              {:ok, status} -> {status, nil, %{}}
              {:error, status, _msg} -> {status, nil, %{}}
            end
          end)

        0x0020 ->
          # C-FIND-RQ
          callback_state = callback_state_for_message(state, message)

          invoke_handler(:handle_find, command_field, state, fn ->
            case handler.handle_find(message.command, message.data, callback_state) do
              {:ok, results} -> send_find_results(results, message, state)
              {:error, status, _msg} -> {status, nil, %{}}
            end
          end)

        0x0010 ->
          # C-GET-RQ
          dispatch_get_request(handler, message, state)

        0x0021 ->
          # C-MOVE-RQ
          dispatch_move_request(handler, message, state)

        0x0FFF ->
          # C-CANCEL-RQ (PS3.7 S9.3.2.3) -- cancels a pending C-FIND/C-MOVE/C-GET.
          # With synchronous handlers, if we're processing this, the operation
          # already completed. No separate response is sent for C-CANCEL.
          :no_response

        # --- DIMSE-N services (PS3.7 Chapter 10) ---

        0x0110 ->
          dispatch_n_request(handler, :handle_n_get, 2, message, state)

        0x0120 ->
          dispatch_n_request(handler, :handle_n_set, 3, message, state)

        0x0130 ->
          dispatch_n_request(handler, :handle_n_action, 3, message, state)

        0x0140 ->
          dispatch_n_request(handler, :handle_n_create, 3, message, state)

        0x0150 ->
          dispatch_n_request(handler, :handle_n_delete, 2, message, state)

        0x0100 ->
          dispatch_n_request(handler, :handle_n_event_report, 3, message, state)

        _ ->
          # Unsupported command
          {0xC000, nil, %{}}
      end

    duration = System.monotonic_time(:millisecond) - start_time

    # C-CANCEL has no response; skip response for :no_response
    # :async_sub_operation means sub-ops are being processed via handle_info
    case result do
      :no_response ->
        Telemetry.emit(:command_stop, %{duration: duration}, %{
          association_id: state.association_id,
          command_field: command_field,
          status: :cancelled
        })

        process_pdv_items(remaining_pdvs, state)

      {:async_sub_operation, new_state} ->
        Telemetry.emit(:command_stop, %{duration: duration}, %{
          association_id: state.association_id,
          command_field: command_field,
          status: :sub_operations
        })

        process_pdv_items(remaining_pdvs, new_state)

      {status, response_data, extra_tags} ->
        Telemetry.emit(:command_stop, %{duration: duration}, %{
          association_id: state.association_id,
          command_field: command_field,
          status: status
        })

        response_field = Bitwise.bor(command_field, 0x8000)

        sop_class_uid =
          Command.affected_sop_class_uid(message.command) ||
            Map.get(message.command, {0x0000, 0x0003}) || ""

        data_set_type = if response_data, do: 0x0000, else: 0x0101

        response_command =
          %{
            {0x0000, 0x0002} => sop_class_uid,
            {0x0000, 0x0100} => response_field,
            {0x0000, 0x0120} => message_id,
            {0x0000, 0x0800} => data_set_type,
            {0x0000, 0x0900} => status
          }
          |> maybe_put_instance_uid(command_field, message.command)
          |> merge_extra_tags(extra_tags)

        pdus =
          Message.fragment(
            response_command,
            response_data,
            message.context_id,
            state.max_pdu_length
          )

        Enum.each(pdus, &send_pdu(state, &1))
        process_pdv_items(remaining_pdvs, state)
    end
  end

  defp send_find_results(results, message, state) do
    # Send each result with Pending status, then final Success
    Enum.each(results, fn result_data ->
      pending_command = %{
        {0x0000, 0x0002} => Command.affected_sop_class_uid(message.command) || "",
        {0x0000, 0x0100} => Fields.c_find_rsp(),
        {0x0000, 0x0120} => Command.message_id(message.command) || 0,
        {0x0000, 0x0800} => 0x0000,
        {0x0000, 0x0900} => 0xFF00
      }

      pdus =
        Message.fragment(pending_command, result_data, message.context_id, state.max_pdu_length)

      Enum.each(pdus, &send_pdu(state, &1))
    end)

    # Final success (no data set)
    {0x0000, nil, %{}}
  end

  # --- C-GET SCU: handle incoming C-STORE-RQ in get_mode ---

  defp handle_get_mode_store(message, state, remaining_pdvs) do
    message_id = Command.message_id(message.command) || 0

    store_rsp =
      %{
        {0x0000, 0x0002} => Command.affected_sop_class_uid(message.command) || "",
        {0x0000, 0x0100} => Fields.c_store_rsp(),
        {0x0000, 0x0120} => message_id,
        {0x0000, 0x0800} => 0x0101,
        {0x0000, 0x0900} => 0x0000
      }
      |> maybe_put_instance_uid(0x0001, message.command)

    pdus = Message.fragment(store_rsp, nil, message.context_id, state.max_pdu_length)
    Enum.each(pdus, &send_pdu(state, &1))

    new_results =
      if message.data,
        do: [message.data | state.pending_results],
        else: state.pending_results

    process_pdv_items(remaining_pdvs, %{state | pending_results: new_results})
  end

  # --- C-GET SCP dispatch ---

  defp dispatch_get_request(handler, message, state) do
    callback_state = callback_state_for_message(state, message)

    case invoke_handler(:handle_get, 0x0010, state, fn ->
           handler.handle_get(message.command, message.data, callback_state)
         end) do
      {:ok, []} ->
        {0x0000, nil, %{}}

      {:ok, instances} ->
        sop_class_uid = Command.affected_sop_class_uid(message.command) || ""
        message_id = Command.message_id(message.command) || 0

        Telemetry.emit_event([:sub_operation, :start], %{system_time: System.system_time()}, %{
          association_id: state.association_id,
          type: :c_get,
          total_instances: length(instances)
        })

        sub_op = %{
          type: :c_get,
          message_id: message_id,
          context_id: message.context_id,
          sop_class_uid: sop_class_uid,
          remaining: instances,
          completed: 0,
          failed: 0,
          warning: 0,
          sub_assoc: nil
        }

        Process.send(self(), {:sub_operation, :next}, [])
        {:async_sub_operation, %{state | sub_operation: sub_op}}

      {:error, status, _msg} ->
        {status, nil, %{}}
    end
  end

  # --- C-MOVE SCP dispatch ---

  defp dispatch_move_request(handler, message, state) do
    move_destination = Map.get(message.command, {0x0000, 0x0600}, "")
    callback_state = callback_state_for_message(state, message)

    case invoke_handler(:handle_move, 0x0021, state, fn ->
           handler.handle_move(message.command, message.data, callback_state)
         end) do
      {:ok, []} ->
        {0x0000, nil, %{}}

      {:ok, instances} ->
        resolve_result =
          if function_exported?(handler, :resolve_ae, 1) do
            handler.resolve_ae(move_destination)
          else
            {:error, :unknown_ae}
          end

        case resolve_result do
          {:ok, {host, port}} ->
            sop_classes = instances |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

            case Dimse.Scu.open(host, port,
                   calling_ae: state.local_ae_title,
                   called_ae: move_destination,
                   abstract_syntaxes: sop_classes,
                   timeout: 5_000
                 ) do
              {:ok, sub_assoc} ->
                wait_for_sub_assoc(sub_assoc)

                sop_class_uid = Command.affected_sop_class_uid(message.command) || ""
                message_id = Command.message_id(message.command) || 0

                Telemetry.emit_event(
                  [:sub_operation, :start],
                  %{system_time: System.system_time()},
                  %{
                    association_id: state.association_id,
                    type: :c_move,
                    total_instances: length(instances)
                  }
                )

                sub_op = %{
                  type: :c_move,
                  message_id: message_id,
                  context_id: message.context_id,
                  sop_class_uid: sop_class_uid,
                  remaining: instances,
                  completed: 0,
                  failed: 0,
                  warning: 0,
                  sub_assoc: sub_assoc
                }

                Process.send(self(), {:sub_operation, :next}, [])
                {:async_sub_operation, %{state | sub_operation: sub_op}}

              {:error, _reason} ->
                {0xA801, nil, %{}}
            end

          {:error, _} ->
            {0xA801, nil, %{}}
        end

      {:error, status, _msg} ->
        {status, nil, %{}}
    end
  end

  defp wait_for_sub_assoc(assoc, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_sub_assoc(assoc, deadline)
  end

  defp do_wait_sub_assoc(assoc, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      :timeout
    else
      case Dimse.Association.negotiated_contexts(assoc) do
        contexts when map_size(contexts) > 0 ->
          :ok

        _ ->
          Process.sleep(5)
          do_wait_sub_assoc(assoc, deadline)
      end
    end
  end

  # --- Sub-operation response helpers ---

  defp send_sub_op_pending_response(sub_op, state) do
    total = length(sub_op.remaining)

    pending_command = %{
      {0x0000, 0x0002} => sub_op.sop_class_uid,
      {0x0000, 0x0100} => sub_op_response_field(sub_op.type),
      {0x0000, 0x0120} => sub_op.message_id,
      {0x0000, 0x0800} => 0x0101,
      {0x0000, 0x0900} => 0xFF00,
      {0x0000, 0x1020} => total,
      {0x0000, 0x1021} => sub_op.completed,
      {0x0000, 0x1022} => sub_op.failed,
      {0x0000, 0x1023} => sub_op.warning
    }

    pdus = Message.fragment(pending_command, nil, sub_op.context_id, state.max_pdu_length)
    Enum.each(pdus, &send_pdu(state, &1))
  end

  defp send_sub_op_final_response(sub_op, state) do
    final_status = if sub_op.failed > 0, do: 0xB000, else: 0x0000

    final_command = %{
      {0x0000, 0x0002} => sub_op.sop_class_uid,
      {0x0000, 0x0100} => sub_op_response_field(sub_op.type),
      {0x0000, 0x0120} => sub_op.message_id,
      {0x0000, 0x0800} => 0x0101,
      {0x0000, 0x0900} => final_status,
      {0x0000, 0x1020} => 0,
      {0x0000, 0x1021} => sub_op.completed,
      {0x0000, 0x1022} => sub_op.failed,
      {0x0000, 0x1023} => sub_op.warning
    }

    pdus = Message.fragment(final_command, nil, sub_op.context_id, state.max_pdu_length)
    Enum.each(pdus, &send_pdu(state, &1))
  end

  defp sub_op_response_field(:c_get), do: Fields.c_get_rsp()
  defp sub_op_response_field(:c_move), do: Fields.c_move_rsp()

  # --- Socket I/O ---

  defp send_pdu(state, pdu) do
    iodata = Encoder.encode(pdu)
    byte_count = :erlang.iolist_size(iodata)

    case state.transport do
      :gen_tcp -> :gen_tcp.send(state.socket, iodata)
      :ssl -> :ssl.send(state.socket, iodata)
      transport -> transport.send(state.socket, iodata)
    end

    Telemetry.emit(:pdu_sent, %{byte_size: byte_count}, %{
      association_id: state.association_id,
      pdu_type: pdu.__struct__
    })
  end

  defp reactivate_socket(%{transport: :gen_tcp, socket: socket}) do
    :inet.setopts(socket, active: :once)
  end

  defp reactivate_socket(%{transport: :ssl, socket: socket}) do
    :ssl.setopts(socket, active: :once)
  end

  defp reactivate_socket(%{transport: transport, socket: socket}) do
    transport.setopts(socket, active: :once)
  end

  defp close_socket(%{socket: nil}), do: :ok
  defp close_socket(%{transport: :gen_tcp, socket: socket}), do: :gen_tcp.close(socket)
  defp close_socket(%{transport: :ssl, socket: socket}), do: :ssl.close(socket)
  defp close_socket(%{transport: transport, socket: socket}), do: transport.close(socket)

  defp close_connection(state, reason) do
    if state.pending_request do
      GenServer.reply(state.pending_request, {:error, reason})
    end

    if state.pending_release do
      GenServer.reply(state.pending_release, {:error, reason})
    end

    {:stop, reason, %{state | phase: :closed, pending_request: nil, pending_release: nil}}
  end

  # --- ARTIM timer ---

  defp start_artim_timer(state) do
    timer = Process.send_after(self(), :artim_timeout, state.config.artim_timeout)
    %{state | artim_timer: timer}
  end

  defp cancel_artim_timer(%{artim_timer: nil}), do: :ok
  defp cancel_artim_timer(%{artim_timer: ref}), do: Process.cancel_timer(ref)

  # --- Helpers ---

  defp build_associate_rq(
         calling_ae,
         called_ae,
         abstract_syntaxes,
         transfer_syntaxes,
         config,
         opts
       ) do
    pcs =
      abstract_syntaxes
      |> Enum.with_index(1)
      |> Enum.map(fn {as, idx} ->
        %Pdu.PresentationContext{
          id: idx * 2 - 1,
          abstract_syntax: as,
          transfer_syntaxes: transfer_syntaxes
        }
      end)

    %Pdu.AssociateRq{
      protocol_version: 1,
      called_ae_title: called_ae,
      calling_ae_title: calling_ae,
      presentation_contexts: pcs,
      user_information: %Pdu.UserInformation{
        max_pdu_length: config.max_pdu_length,
        implementation_uid: @implementation_uid,
        implementation_version: @implementation_version,
        role_selections: Keyword.get(opts, :role_selections),
        user_identity: Keyword.get(opts, :user_identity)
      }
    }
  end

  defp handler_abstract_syntaxes(nil), do: MapSet.new(["1.2.840.10008.1.1"])

  defp handler_abstract_syntaxes(handler) do
    case Code.ensure_loaded(handler) do
      {:module, ^handler} ->
        if function_exported?(handler, :supported_abstract_syntaxes, 0) do
          handler.supported_abstract_syntaxes() |> MapSet.new()
        else
          MapSet.new(["1.2.840.10008.1.1"])
        end

      {:error, _reason} ->
        MapSet.new(["1.2.840.10008.1.1"])
    end
  end

  defp send_dimse_request(command_set, data, state, on_sent) do
    # DIMSE-N services use RequestedSOPClassUID (0000,0003) instead of Affected (0000,0002)
    sop_class =
      Map.get(command_set, {0x0000, 0x0002}) || Map.get(command_set, {0x0000, 0x0003})

    context_id = find_context_id(state.negotiated_contexts, sop_class)

    if context_id do
      pdus = Message.fragment(command_set, data, context_id, state.max_pdu_length)
      Enum.each(pdus, &send_pdu(state, &1))
      on_sent.()
    else
      {:reply, {:error, :no_accepted_context}, state}
    end
  end

  defp find_context_id(contexts, sop_class) do
    Enum.find_value(contexts, fn
      {id, {abstract_syntax, _ts}} ->
        if compatible_sop_class?(abstract_syntax, sop_class), do: id

      _ ->
        nil
    end)
  end

  defp request_on_negotiated_context?(%Message{context_id: context_id, command: command}, state) do
    case Map.get(state.negotiated_contexts, context_id) do
      {abstract_syntax, _transfer_syntax} ->
        # Check both Affected (0000,0002) and Requested (0000,0003) SOP Class UIDs
        sop_class_uid =
          Command.affected_sop_class_uid(command) || Map.get(command, {0x0000, 0x0003})

        case sop_class_uid do
          nil -> true
          uid -> compatible_sop_class?(abstract_syntax, uid)
        end

      nil ->
        false
    end
  end

  defp compatible_sop_class?(uid, uid), do: true

  defp compatible_sop_class?("1.2.840.10008.5.1.1.9", sop_class_uid) do
    sop_class_uid in [
      "1.2.840.10008.5.1.1.1",
      "1.2.840.10008.5.1.1.2",
      "1.2.840.10008.5.1.1.4",
      "1.2.840.10008.5.1.1.16"
    ]
  end

  defp compatible_sop_class?("1.2.840.10008.5.1.1.18", sop_class_uid) do
    sop_class_uid in [
      "1.2.840.10008.5.1.1.1",
      "1.2.840.10008.5.1.1.2",
      "1.2.840.10008.5.1.1.4.1",
      "1.2.840.10008.5.1.1.16"
    ]
  end

  defp compatible_sop_class?(_abstract_syntax, _sop_class_uid), do: false

  defp get_in_user_info(%{user_information: %Pdu.UserInformation{} = ui}, field) do
    Map.get(ui, field)
  end

  defp get_in_user_info(_, _), do: nil

  defp generate_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  defp proposed_contexts(abstract_syntaxes) do
    abstract_syntaxes
    |> Enum.with_index(1)
    |> Map.new(fn {abstract_syntax, idx} -> {idx * 2 - 1, abstract_syntax} end)
  end

  # --- DIMSE-N SCP dispatch helper ---

  defp dispatch_n_request(handler, callback, arity, message, state) do
    if function_exported?(handler, callback, arity) do
      callback_state = callback_state_for_message(state, message)

      result =
        case arity do
          2 -> apply(handler, callback, [message.command, callback_state])
          3 -> apply(handler, callback, [message.command, message.data, callback_state])
        end

      normalize_n_dispatch_result(callback, result, message.command)
    else
      # No Such SOP Class (PS3.7 Annex C)
      {0x0112, nil, %{}}
    end
  end

  defp normalize_n_dispatch_result(
         :handle_n_create,
         {:ok, status, sop_instance_uid, data},
         command
       )
       when is_binary(sop_instance_uid) do
    extra_tags =
      command
      |> build_n_response_extra_tags()
      |> Map.put({0x0000, 0x1000}, sop_instance_uid)

    {status, data, extra_tags}
  end

  defp normalize_n_dispatch_result(_callback, {:ok, status, data}, command) do
    {status, data, build_n_response_extra_tags(command)}
  end

  defp normalize_n_dispatch_result(_callback, {:ok, status}, command) do
    {status, nil, build_n_response_extra_tags(command)}
  end

  defp normalize_n_dispatch_result(_callback, {:error, status, _msg}, _command) do
    {status, nil, %{}}
  end

  defp build_n_response_extra_tags(command) do
    instance_uid = Map.get(command, {0x0000, 0x1001}) || Map.get(command, {0x0000, 0x1000})

    [
      {{0x0000, 0x1000}, instance_uid},
      {{0x0000, 0x1002}, Map.get(command, {0x0000, 0x1002})},
      {{0x0000, 0x1008}, Map.get(command, {0x0000, 0x1008})}
    ]
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  defp merge_extra_tags(cmd, extra_tags) when map_size(extra_tags) == 0, do: cmd
  defp merge_extra_tags(cmd, extra_tags), do: Map.merge(cmd, extra_tags)

  # --- Extended Negotiation helpers ---

  # Authenticate the requesting SCU. Returns {:ok, user_identity_ac | nil} or {:error, reason}.
  defp authenticate_user(nil, _handler, _state), do: {:ok, nil}

  defp authenticate_user(%Pdu.UserInformation{user_identity: nil}, _handler, _state) do
    {:ok, nil}
  end

  defp authenticate_user(%Pdu.UserInformation{user_identity: identity}, handler, state) do
    if function_exported?(handler, :handle_authenticate, 2) do
      case handler.handle_authenticate(identity, state) do
        {:ok, nil} ->
          {:ok, nil}

        {:ok, server_response} when is_binary(server_response) ->
          if identity.positive_response_requested do
            {:ok, %Pdu.UserIdentityAc{server_response: server_response}}
          else
            {:ok, nil}
          end

        {:error, _} = err ->
          err
      end
    else
      {:ok, nil}
    end
  end

  defp validate_association_request(rq, handler, state) do
    if function_exported?(handler, :validate_association, 2) do
      case handler.validate_association(rq, state) do
        {:ok, nil} -> :ok
        {:error, _} = error -> error
      end
    else
      :ok
    end
  end

  # Echo back role selections that were accepted (filtered to negotiated SOP classes).
  defp echo_role_selections(%Pdu.UserInformation{role_selections: nil}, _accepted), do: nil
  defp echo_role_selections(%Pdu.UserInformation{role_selections: []}, _accepted), do: nil

  defp echo_role_selections(%Pdu.UserInformation{role_selections: roles}, accepted) do
    accepted_sop_uids =
      accepted
      |> Map.values()
      |> Enum.map(fn {sop_class_uid, _ts} -> sop_class_uid end)
      |> MapSet.new()

    filtered =
      Enum.filter(roles, fn rs -> MapSet.member?(accepted_sop_uids, rs.sop_class_uid) end)

    if filtered == [], do: nil, else: filtered
  end

  # Convert a list of RoleSelection structs to the state's role_selections map.
  defp roles_to_map(nil), do: %{}

  defp roles_to_map(roles) do
    Map.new(roles, fn rs -> {rs.sop_class_uid, {rs.scu_role, rs.scp_role}} end)
  end

  # C-STORE-RSP must echo back the AffectedSOPInstanceUID (PS3.7 Table 9.1-1)
  defp maybe_put_instance_uid(cmd, 0x0001, request_command) do
    case Map.get(request_command, {0x0000, 0x1000}) do
      nil -> cmd
      uid -> Map.put(cmd, {0x0000, 0x1000}, uid)
    end
  end

  defp maybe_put_instance_uid(cmd, _command_field, _request_command), do: cmd

  # --- Telemetry helpers ---

  # Wraps a handler callback invocation with handler telemetry events.
  defp invoke_handler(callback, command_field, state, fun) do
    Telemetry.emit_event([:handler, :start], %{system_time: System.system_time()}, %{
      association_id: state.association_id,
      callback: callback,
      command_field: command_field
    })

    handler_start = System.monotonic_time(:millisecond)

    try do
      result = fun.()
      handler_duration = System.monotonic_time(:millisecond) - handler_start

      status =
        case result do
          {s, _, _} when is_integer(s) -> s
          _ -> 0x0000
        end

      Telemetry.emit_event([:handler, :stop], %{duration: handler_duration}, %{
        association_id: state.association_id,
        callback: callback,
        status: status
      })

      result
    rescue
      e ->
        handler_duration = System.monotonic_time(:millisecond) - handler_start

        Telemetry.emit_event([:handler, :exception], %{duration: handler_duration}, %{
          association_id: state.association_id,
          callback: callback,
          kind: :error,
          reason: e
        })

        reraise e, __STACKTRACE__
    catch
      kind, reason ->
        handler_duration = System.monotonic_time(:millisecond) - handler_start

        Telemetry.emit_event([:handler, :exception], %{duration: handler_duration}, %{
          association_id: state.association_id,
          callback: callback,
          kind: kind,
          reason: reason
        })

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  # Emit sub-operation progress event.
  defp emit_sub_op_progress(sub_op, state) do
    Telemetry.emit_event([:sub_operation, :progress], %{}, %{
      association_id: state.association_id,
      type: sub_op.type,
      completed: sub_op.completed,
      failed: sub_op.failed,
      remaining: length(sub_op.remaining)
    })
  end

  # Emit TLS handshake event with connection information.
  defp emit_tls_handshake(socket, association_id) do
    {protocol_version, cipher_suite} =
      case :ssl.connection_information(socket) do
        {:ok, info} ->
          {Keyword.get(info, :protocol), Keyword.get(info, :selected_cipher_suite)}

        _ ->
          {nil, nil}
      end

    Telemetry.emit_event([:tls, :handshake], %{}, %{
      association_id: association_id,
      protocol_version: protocol_version,
      cipher_suite: cipher_suite
    })
  end
end
