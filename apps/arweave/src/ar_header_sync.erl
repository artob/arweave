-module(ar_header_sync).

-behaviour(gen_server).

-export([
	start_link/1,
	record_written_block/3
]).

-export([init/1, handle_cast/2, handle_call/3, terminate/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

%% The frequency of processing items in the queue.
-define(PROCESS_ITEM_INTERVAL_MS, 150).
%% The initial value for the exponential backoff for failing requests.
-define(INITIAL_BACKOFF_INTERVAL_S, 30).
%% The maximum exponential backoff interval for failing requests.
-define(MAX_BACKOFF_INTERVAL_S, 2 * 60 * 60).

%%% This module syncs block and transaction headers and maintains a persisted record of synced
%%% headers. Headers are synced from latest to earliest. Includes a migration process that
%%% moves data to v2 index for blocks written prior to the 2.1 update.

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

record_written_block(H, Height, PrevH) ->
	gen_server:cast(?MODULE, {record_written_block, H, Height, PrevH}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([{node, Node}]) ->
	ar:info([{event, ar_header_sync_start}]),
	process_flag(trap_exit, true),
	{ok, DB} = ar_kv:open("ar_header_sync_db"),
	SyncRecord =
		case ar_storage:read_term(header_sync_record) of
			not_found ->
				ar_intervals:new();
			{ok, StoredSyncRecord} ->
				StoredSyncRecord
		end,
	gen_server:cast(?MODULE, process_item),
	{ok,
		#{
			queue => queue:new(),
			db => DB,
			sync_record => SyncRecord,
			last_picked => no_height,
			node => Node
		}}.

handle_cast({record_written_block, H, Height, PrevH}, State) ->
	#{ db := DB, sync_record := SyncRecord } = State,
	case ar_kv:put(DB, << Height:256 >>, term_to_binary({H, PrevH})) of
		ok ->
			UpdatedSyncRecord = ar_intervals:add(SyncRecord, Height, Height - 1),
			case ar_storage:write_term(header_sync_record, UpdatedSyncRecord) of
				ok ->
					prometheus_gauge:set(synced_blocks, ar_kv:count(DB)),
					{noreply, State#{ sync_record => UpdatedSyncRecord }};
				{error, Reason} ->
					ar:err([
						{event, ar_header_sync_failed_to_update_sync_record},
						{block, ar_util:encode(H)},
						{height, Height},
						{reason, Reason}
					]),
					{noreply, State}
			end;
		{error, Reason} ->
			ar:err([
				{event, ar_header_sync_failed_to_record_written_block},
				{block, ar_util:encode(H)},
				{height, Height},
				{reason, Reason}
			]),
			{noreply, State}
	end;

handle_cast(process_item, State) ->
	#{
		queue := Queue,
		last_picked := LastPicked,
		db := DB,
		sync_record := SyncRecord,
		node := Node
	} = State,
	prometheus_gauge:set(downloader_queue_size, queue:len(Queue)),
	UpdatedQueue = process_item(Queue),
	timer:apply_after(?PROCESS_ITEM_INTERVAL_MS, gen_server, cast, [?MODULE, process_item]),
	case pick_unsynced_block(LastPicked, DB, SyncRecord) of
		nothing_to_sync ->
			{noreply, State#{ queue => UpdatedQueue}};
		{ok, Height, H} ->
			case ar_node:get_tx_root(Node, H) of
				not_found ->
					ar:err([
						{event, ar_header_sync_tx_root_not_found},
						{block, ar_util:encode(H)},
						{height, Height},
						{sync_record, SyncRecord}
					]),
					{noreply, State#{ queue => UpdatedQueue}};
				TXRoot ->
					{noreply, State#{
						queue => enqueue_front({block, H, TXRoot}, UpdatedQueue),
						last_picked => Height
					}}
			end
	end.

handle_call(_, _, _) ->
	not_implemented.

terminate(Reason, #{ db := DB }) ->
	ar:info([{event, ar_header_sync_terminate}, {reason, Reason}]),
	ar_kv:close(DB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

pick_unsynced_block(1, DB, SyncRecord) ->
	pick_unsynced_block(no_height, DB, SyncRecord);
pick_unsynced_block(LastPicked, DB, SyncRecord) ->
	case ar_intervals:is_empty(SyncRecord) of
		true ->
			nothing_to_sync;
		false ->
			{{_End, Start}, SyncRecord2} = ar_intervals:take_largest(SyncRecord),
			case Start > 0 of
				false ->
					nothing_to_sync;
				true ->
					case LastPicked of
						no_height ->
							read_from_kv(DB, Start);
						_ ->
							case LastPicked - 1 > Start of
								true ->
									read_from_kv(DB, Start);
								false ->
									pick_unsynced_block(LastPicked, DB, SyncRecord2)
							end
					end
			end
	end.

read_from_kv(DB, Start) ->
	case ar_kv:get(DB, << (Start + 1):256 >>) of
		{ok, Value} ->
			{ok, Start, element(2, binary_to_term(Value))};
		Error ->
			Error
	end.

enqueue_front(Item, Queue) ->
	queue:in_r({Item, initial_backoff()}, Queue).

initial_backoff() ->
	{os:system_time(seconds) + ?INITIAL_BACKOFF_INTERVAL_S, ?INITIAL_BACKOFF_INTERVAL_S}.

process_item(Queue) ->
	Now = os:system_time(seconds),
	case queue:out(Queue) of
		{empty, _Queue} ->
			Queue;
		{{value, {Item, {BackoffTimestamp, _} = Backoff}}, UpdatedQueue}
				when BackoffTimestamp > Now ->
			enqueue_back(Item, Backoff, UpdatedQueue);
		{{value, {{block, H, TXRoot}, Backoff}}, UpdatedQueue} ->
			case download_block(H, TXRoot) of
				{error, _Reason} ->
					UpdatedBackoff = update_backoff(Now, Backoff),
					enqueue_back({block, {H, TXRoot}}, UpdatedBackoff, UpdatedQueue);
				{ok, B} ->
					store_block(B),
					UpdatedQueue
			end
	end.

enqueue_back(Item, Backoff, Queue) ->
	queue:in({Item, Backoff}, Queue).

update_backoff(Now, {_Timestamp, Interval}) ->
	UpdatedInterval = min(?MAX_BACKOFF_INTERVAL_S, Interval * 2),
	{Now + UpdatedInterval, UpdatedInterval}.

download_block(H, TXRoot) ->
	Peers = ar_bridge:get_remote_peers(whereis(http_bridge_node)),
	case ar_storage:read_block(H) of
		unavailable ->
			download_block(Peers, H, TXRoot);
		B ->
			download_txs(Peers, B, TXRoot)
	end.

download_block(Peers, H, TXRoot) when is_binary(H) ->
	Fork_2_0 = ar_fork:height_2_0(),
	case ar_http_iface_client:get_block_shadow(Peers, H) of
		unavailable ->
			ar:warn([
				{event, ar_header_sync_failed_to_download_block_header},
				{block, ar_util:encode(H)}
			]),
			{error, block_header_unavailable};
		{Peer, #block{ height = Height } = B} when Height >= Fork_2_0 ->
			case ar_weave:indep_hash(B) of
				H ->
					download_txs(Peers, B, TXRoot);
				_ ->
					ar:warn([
						{event, ar_header_sync_block_hash_mismatch},
						{block, ar_util:encode(H)},
						{peer, ar_util:format_peer(Peer)}
					]),
					{error, block_hash_mismatch}
			end;
		{_Peer, B} ->
			download_txs(Peers, B, TXRoot)
	end.

download_txs(Peers, B, TXRoot) ->
	case ar_http_iface_client:get_txs(Peers, #{}, B) of
		{ok, TXs} ->
			case verify_txs(B, TXs) of
				false ->
					{error, tx_id_mismatch};
				true ->
					SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
					SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
					{Root, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
					case Root of
						TXRoot ->
							ar_data_sync:add_historical_block(B, SizeTaggedTXs),
							move_data_to_v2_index(TXs),
							case store_txs(B#block.indep_hash, TXs) of
								ok ->
									{ok, B};
								Error ->
									Error
							end;
						_ ->
							ar:warn([
								{event, ar_header_sync_block_tx_root_mismatch},
								{block, ar_util:encode(B#block.indep_hash)}
							]),
							{error, block_tx_root_mismatch}
					end
			end;
		{error, txs_exceed_block_size_limit} ->
			ar:warn([
				{event, ar_header_sync_block_txs_exceed_block_size_limit},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			{error, txs_exceed_block_size_limit};
		{error, txs_count_exceeds_limit} ->
			ar:warn([
				{event, ar_header_sync_block_txs_count_exceeds_limit},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			{error, txs_count_exceeds_limit};
		{error, tx_not_found} ->
			ar:warn([
				{event, ar_header_sync_block_tx_not_found},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			{error, tx_not_found}
	end.

verify_txs(B, TXs) ->
	lists:foldl(
		fun ({TXID, TX}, ok) ->
				case ar_tx:verify_tx_id(TXID, TX) of
					true ->
						ok;
					false ->
						ar:warn([
							{event, ar_header_sync_tx_id_mismatch},
							{tx, ar_util:encode(TXID)}
						]),
						invalid
				end;
			(_Pair, invalid) ->
				invalid
		end,
		ok,
		lists:zip(B#block.txs, TXs)
	).

move_data_to_v2_index(TXs) ->
	%% Migrate the transaction data to the new index for blocks
	%% written prior to this update.
	lists:foldl(
		fun (#tx{ format = 2, data_size = DataSize, data = Data } = TX, ok)
					when DataSize > 0 ->
				case ar_storage:read_tx_data(TX) of
					{error, enoent} ->
						ok;
					{ok, Data} ->
						case ar_storage:write_tx_data(Data) of
							ok ->
								file:delete(ar_storage:tx_data_filepath(TX));
							Error ->
								Error
						end;
					Error ->
						Error
				end;
			(_, Acc) ->
				Acc
		end,
		ok,
		TXs
	).

store_txs(BH, TXs) ->
	StoreTags = case ar_meta_db:get(arql_tags_index) of
		true ->
			store_tags;
		_ ->
			do_not_store_tags
	end,
	lists:foldl(
		fun (TX, ok) ->
				case ar_storage:lookup_tx_filename(TX) of
					unavailable ->
						case ar_storage:write_tx(TX) of
							ok ->
								ar_arql_db:insert_tx(BH, TX, StoreTags);
							{error, Reason} = Error ->
								ar:warn([
									{event, ar_header_sync_failed_to_write_tx},
									{tx, ar_util:encode(TX#tx.id)},
									{reason, Reason}
								]),
								Error
						end;
					_ ->
						ok
				end;
			(_TX, Error) ->
				Error
		end,
		ok,
		TXs
	).

store_block(B) ->
	case ar_storage:lookup_block_filename(B#block.indep_hash) of
		unavailable ->
			case ar_storage:write_block(B) of
				ok ->
					ar_arql_db:insert_block(B);
				{error, Reason} = Error ->
					ar:warn([
						{event, ar_header_sync_failed_to_write_block},
						{block, ar_util:encode(B#block.indep_hash)},
						{reason, Reason}
					]),
					Error
			end;
		_ ->
			H = B#block.indep_hash,
			Height = B#block.height,
			PrevH = B#block.previous_block,
			gen_server:cast(self(), {record_written_block, H, Height, PrevH})
	end.
