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

import io.datakernel.async.service.EventloopService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.crdt.*;
import io.datakernel.crdt.primitives.CrdtType;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.stats.StreamStats;
import io.datakernel.datastream.stats.StreamStatsBasic;
import io.datakernel.datastream.stats.StreamStatsDetailed;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.jmx.stats.EventStats;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.BinaryInput;
import io.datakernel.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;

import java.time.Duration;
import java.util.concurrent.Executor;

public final class CrdtStorageRocksDB<K extends Comparable<K>, S> implements CrdtStorage<K, S>, EventloopService, EventloopJmxMBeanEx {
	private final Eventloop eventloop;
	private final Executor executor;
	private final RocksDB db;
	private final CrdtFunction<S> function;
	private final BinarySerializer<K> keySerializer;
	private final BinarySerializer<S> stateSerializer;

	private final FlushOptions flushOptions; // } are closed by GC when the client is destroyed
	private final WriteOptions writeOptions; // }

	private MemSize bufferSize = MemSize.kilobytes(16);
	private CrdtFilter<S> filter = $ -> true;

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

	private CrdtStorageRocksDB(Eventloop eventloop, Executor executor, RocksDB db,
			BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer, CrdtFunction<S> function) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.db = db;
		this.function = function;
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		flushOptions = new FlushOptions();
		writeOptions = new WriteOptions().setDisableWAL(true);
	}

	public static <K extends Comparable<K>, S> CrdtStorageRocksDB<K, S> create(
			Eventloop eventloop, Executor executor, RocksDB db,
			CrdtDataSerializer<K, S> serializer, CrdtFunction<S> crdtFunction) {
		return new CrdtStorageRocksDB<>(eventloop, executor, db, serializer.getKeySerializer(), serializer.getStateSerializer(), crdtFunction);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> CrdtStorageRocksDB<K, S> create(
			Eventloop eventloop, Executor executor, RocksDB db, CrdtDataSerializer<K, S> serializer) {
		return new CrdtStorageRocksDB<>(eventloop, executor, db, serializer.getKeySerializer(), serializer.getStateSerializer(), CrdtFunction.ofCrdtType());
	}

	public CrdtStorageRocksDB<K, S> withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public CrdtStorageRocksDB<K, S> withFilter(CrdtFilter<S> filter) {
		this.filter = filter;
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
			state = function.merge(state, stateSerializer.decode(possibleState, 0));
			if (!filter.test(state)) {
				try {
					db.delete(keyBytes);
				} catch (RocksDBException e) {
					throw new UncheckedException(e);
				}
				buf.recycle();
				return;
			}
		}
		buf.rewind();
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
						.withAcknowledgement(ack -> ack.then(this::flush))));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
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

								S partial = function.extract(stateSerializer.decode(stateBytes, 0), timestamp);
								if (partial != null) {
									return new CrdtData<>(keySerializer.decode(keyBytes, 0), partial);
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
						.withAcknowledgement(ack -> ack.then(this::flush))));
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

	public Promise<S> get(K key) {
		return Promise.ofBlockingCallable(executor, () -> {
			ByteBuf buf = ByteBufPool.allocate(bufferSize);
			keySerializer.encode(buf.array(), buf.head(), key);
			byte[] state = db.get(buf.asArray());
			if (state == null) {
				return null;
			}
			singleGets.recordEvent();
			return stateSerializer.decode(new BinaryInput(state));
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
