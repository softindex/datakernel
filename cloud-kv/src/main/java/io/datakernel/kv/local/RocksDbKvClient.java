/*
 * Copyright (C) 2015-2020 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.kv.local;

import io.datakernel.async.service.EventloopService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.kv.KvClient;
import io.datakernel.kv.KvItem;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.stats.StreamStats;
import io.datakernel.datastream.stats.StreamStatsBasic;
import io.datakernel.datastream.stats.StreamStatsDetailed;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventStats;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;

public final class RocksDbKvClient<K, V> implements KvClient<K, V>, EventloopService, EventloopJmxMBeanEx {
	private final Eventloop eventloop;
	private final Executor executor;
	private final RocksDB db;
	private final StructuredCodec<K> keyCodec;
	private final StructuredCodec<V> valueCodec;

	private final FlushOptions flushOptions; // it closed by the GC when it collects the client
	private final WriteOptions writeOptions; // same as ^

	private MemSize bufferSize = MemSize.kilobytes(16);

	private final Map<String, Promise<ColumnFamilyHandle>> handlesMap = new HashMap<>();

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<KvItem<K, V>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<KvItem<K, V>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<KvItem<K, V>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<KvItem<K, V>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();

	private final EventStats singlePuts = EventStats.create(Duration.ofMinutes(5));
	private final EventStats singleGets = EventStats.create(Duration.ofMinutes(5));
	private final EventStats singleRemoves = EventStats.create(Duration.ofMinutes(5));
	// endregion

	private RocksDbKvClient(Eventloop eventloop, Executor executor, RocksDB db,
			StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.db = db;
		this.keyCodec = keyCodec;
		this.valueCodec = valueCodec;
		flushOptions = new FlushOptions();
		writeOptions = new WriteOptions().setDisableWAL(true);
	}

	public static <K extends Comparable<K>, S> RocksDbKvClient<K, S> create(
			Eventloop eventloop, Executor executor, RocksDB db,
			StructuredCodec<K> keyCodec, StructuredCodec<S> valueCodec) {
		return new RocksDbKvClient<>(eventloop, executor, db, keyCodec, valueCodec);
	}

	public RocksDbKvClient<K, V> withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public RocksDB getDb() {
		return db;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public Promise<Void> flush() {
		return Promise.ofBlockingRunnable(executor, () -> {
			try {
				db.flush(flushOptions);
			} catch (RocksDBException e) {
				throw new UncheckedException(e);
			}
		});
	}

	private Promise<ColumnFamilyHandle> getHandle(String table) {
		return handlesMap.computeIfAbsent(table, $ -> Promise.ofBlockingCallable(executor,
				() -> {
					ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encodeAsArray(STRING_CODEC, table));
					return db.createColumnFamily(cfd);
				})
				.whenException(e -> handlesMap.remove(table))
		);
	}

	private Promise<Void> doPut(String table, K key, V state) {

		byte[] keyBytes = encodeAsArray(keyCodec, key);

//		getHandle(table)
//				.then(handle -> {
//
//				});

		byte[] value = db.get(handle, key);
		if (value != null) {
			SignedData<RawKvItem> old = unpackValue(key, value);
			if (old.getValue().getTimestamp() > kvItem.getValue().getTimestamp()) {
				return;
			}
		}
		db.put(handle, writeOptions, key, packValue(kvItem));

//		ByteBuf buf = ByteBufPool.allocate(bufferSize);
//		buf.tail(keySerializer.encode(buf.array(), buf.tail(), key));
//		byte[] keyBytes = buf.getArray();
//		byte[] possibleState;
//		try {
//			possibleState = db.get(keyBytes);
//		} catch (RocksDBException e) {
//			throw new UncheckedException(e);
//		}
//
////		custom merge operators in RocksJava are yet to come
//		if (possibleState != null) {
//			state = function.merge(state, stateSerializer.decode(possibleState, 0));
//		}
//		buf.rewind();
//		buf.tail(stateSerializer.encode(buf.array(), buf.tail(), state));
//		try {
//			db.put(writeOptions, keyBytes, buf.asArray());
//		} catch (RocksDBException e) {
//			throw new UncheckedException(e);
//		}
	}

	private void doRemove(K key) {
		ByteBuf buf = ByteBufPool.allocate(bufferSize);
		buf.tail(keyCodec.encode(buf.array(), buf.tail(), key));
		try {
			db.delete(writeOptions, buf.asArray());
		} catch (RocksDBException e) {
			throw new UncheckedException(e);
		}
	}

	@Override
	public Promise<StreamConsumer<KvItem<K, V>>> upload() {
		return Promise.of(StreamConsumer.ofChannelConsumer(
				ChannelConsumer.<KvItem<K, V>>of(data -> Promise.ofBlockingRunnable(executor, () -> doPut(data.getKey(), data.getState())))
						.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
						.withAcknowledgement(ack -> ack.then($ -> flush()))));
	}

	@Override
	public Promise<StreamSupplier<KvItem<K, V>>> download(long revision) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					RocksIterator iterator = db.newIterator();
					iterator.seekToFirst();
					return iterator;
				})
				.map(iterator -> StreamSupplier.ofChannelSupplier(ChannelSupplier.of(
						() -> Promise.ofBlockingCallable(executor, () -> {
							while (iterator.isValid()) {
								byte[] keyBytes = iterator.key();
								byte[] stateBytes = iterator.value();
								iterator.next();

								V partial = function.extract(valueCodec.decode(stateBytes, 0), revision);
								if (partial != null) {
									return new KvItem<>(keyCodec.decode(keyBytes, 0), partial);
								}
							}
							return null;
						})))
						.transformWith(detailedStats ? downloadStatsDetailed : downloadStats));
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return Promise.of(StreamConsumer.ofChannelConsumer(
				ChannelConsumer.<K>of(key -> Promise.ofBlockingRunnable(executor, () -> doRemove(key)))
						.transformWith(detailedStats ? removeStatsDetailed : removeStats)
						.withAcknowledgement(ack -> ack.then($ -> flush()))));
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	public Promise<V> get(K key) {
		return Promise.ofBlockingCallable(executor, () -> {
			ByteBuf buf = ByteBufPool.allocate(bufferSize);
			keyCodec.encode(buf.array(), buf.head(), key);
			byte[] state = db.get(buf.asArray());
			if (state == null) {
				return null;
			}
			singleGets.recordEvent();
			return valueCodec.decode(new BinaryInput(state));
		});
	}

	public Promise<Void> put(K key, V state) {
		return Promise.ofBlockingRunnable(executor, () -> {
			doPut(key, state);
			singlePuts.recordEvent();
		});
	}

	public Promise<Void> remove(K key) {
		return Promise.ofBlockingRunnable(executor, () -> {
			doRemove(key);
			singleRemoves.recordEvent();
		});
	}

	public static class KeyComparator<K extends Comparable<K>> extends Comparator {
		private final ComparatorOptions copt;
		private final BinarySerializer<K> keySerializer;

		public KeyComparator(ComparatorOptions copt, BinarySerializer<K> keySerializer) {
			super(copt);
			this.copt = copt;
			this.keySerializer = keySerializer;
		}

		public KeyComparator(BinarySerializer<K> keySerializer) {
			this(new ComparatorOptions(), keySerializer);
		}

		@Override
		public String name() {
			return "CRDT key comparator";
		}

		@Override
		public int compare(Slice s1, Slice s2) {
			return keySerializer.decode(s1.data(), 0)
					.compareTo(keySerializer.decode(s2.data(), 0));
		}

		@Override
		public void close() {
			super.close();
			copt.close();
		}
	}

	// region JMX
	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public StreamStatsBasic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStatsBasic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStatsDetailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}

	@JmxAttribute
	public EventStats getSinglePuts() {
		return singlePuts;
	}

	@JmxAttribute
	public EventStats getSingleGets() {
		return singleGets;
	}

	@JmxAttribute
	public EventStats getSingleRemoves() {
		return singleRemoves;
	}
	// endregion
}
