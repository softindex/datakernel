/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.datakernel.crdt.local;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.CrdtStorage;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.UncheckedException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.util.BinaryInput;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.MemSize;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.function.BinaryOperator;

public final class CrdtStorageRocksDB<K extends Comparable<K>, S> implements CrdtStorage<K, S>, EventloopService, EventloopJmxMBeanEx {
	private final Eventloop eventloop;
	private final Executor executor;
	private final RocksDB db;
	private final BinaryOperator<S> combiner;
	private final BinarySerializer<K> keySerializer;
	private final BinarySerializer<S> stateSerializer;

	private final FlushOptions flushOptions; // } are closed by GC when the client is destroyed
	private final WriteOptions writeOptions; // }

	private MemSize bufferSize = MemSize.kilobytes(16);

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();

	private final EventStats singlePuts = EventStats.create(Duration.ofMinutes(5));
	private final EventStats singleGets = EventStats.create(Duration.ofMinutes(5));
	private final EventStats singleRemoves = EventStats.create(Duration.ofMinutes(5));
	// endregion

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private CrdtStorageRocksDB(Eventloop eventloop, Executor executor, RocksDB db, BinaryOperator<S> combiner,
			BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.db = db;
		this.combiner = combiner;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		flushOptions = new FlushOptions();
		writeOptions = new WriteOptions().setDisableWAL(true);
	}

	public static <K extends Comparable<K>, S> CrdtStorageRocksDB<K, S> create(Eventloop eventloop, Executor executor, RocksDB db,
			BinaryOperator<S> combiner, CrdtDataSerializer<K, S> serializer) {
		return new CrdtStorageRocksDB<>(eventloop, executor, db, combiner, serializer.getKeySerializer(), serializer.getStateSerializer());
	}

	public static <K extends Comparable<K>, S> CrdtStorageRocksDB<K, S> create(Eventloop eventloop, Executor executor, RocksDB db,
			BinaryOperator<S> combiner, BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new CrdtStorageRocksDB<>(eventloop, executor, db, combiner, keySerializer, stateSerializer);
	}

	public CrdtStorageRocksDB withBufferSize(MemSize bufferSize) {
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

	private void doPut(K key, S state) {
		ByteBuf buf = ByteBufPool.allocate(bufferSize);
		buf.tail(keySerializer.encode(buf.array(), buf.tail(), key));
		byte[] keyBytes = buf.getArray();
		byte[] possibleState;
		try {
			possibleState = db.get(keyBytes);
		} catch (RocksDBException e) {
			throw new UncheckedException(e);
		}

		// custom merge operators in RocksJava are yet to come
		if (possibleState != null) {
			ByteBuf stateBuf = ByteBuf.wrap(possibleState, 8, possibleState.length); // 8 is to skip the timestamp
			state = combiner.apply(state, stateSerializer.decode(stateBuf.array(), stateBuf.head()));
		}

		buf.rewind();
		buf.writeLong(now.currentTimeMillis()); // new timestamp
		buf.tail(stateSerializer.encode(buf.array(), buf.tail(), state));
		try {
			db.put(writeOptions, keyBytes, buf.asArray());
		} catch (RocksDBException e) {
			throw new UncheckedException(e);
		}
	}

	private void doRemove(K key) {
		ByteBuf buf = ByteBufPool.allocate(bufferSize);
		buf.tail(keySerializer.encode(buf.array(), buf.tail(), key));
		try {
			db.delete(writeOptions, buf.asArray());
		} catch (RocksDBException e) {
			throw new UncheckedException(e);
		}
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return Promise.of(StreamConsumer.ofChannelConsumer(
				ChannelConsumer.<CrdtData<K, S>>of(data -> Promise.ofBlockingRunnable(executor, () -> doPut(data.getKey(), data.getState())))
						.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
						.withAcknowledgement(ack -> ack.thenCompose($ -> flush()))));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					RocksIterator iterator = db.newIterator();
					iterator.seekToFirst();
					return iterator;
				})
				.thenApply(iterator -> StreamSupplier.ofChannelSupplier(ChannelSupplier.of(
						() -> Promise.ofBlockingCallable(executor, () -> {
							while (iterator.isValid()) {
								byte[] keyBytes = iterator.key();
								BinaryInput stateBuf = new BinaryInput(iterator.value());
								iterator.next();
								long ts = stateBuf.readLong();
								if (ts > timestamp) {
									return new CrdtData<>(
											keySerializer.decode(keyBytes, 0),
											stateSerializer.decode(stateBuf)
									);
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
						.withAcknowledgement(ack -> ack.thenCompose($ -> flush()))));
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> start() {
		return Promise.complete();
	}

	@NotNull
	@Override
	public MaterializedPromise<Void> stop() {
		return Promise.complete();
	}

	public Promise<S> get(K key) {
		return Promise.ofBlockingCallable(executor, () -> {
			ByteBuf buf = ByteBufPool.allocate(bufferSize);
			keySerializer.encode(buf.array(), buf.head(), key);
			byte[] state = db.get(buf.asArray());
			if (state == null) {
				return null;
			}
			BinaryInput stateBuf = new BinaryInput(state, 8); // skip timestamp
			S res = stateSerializer.decode(stateBuf);
			singleGets.recordEvent();
			return res;
		});
	}

	public Promise<Void> put(K key, S state) {
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
