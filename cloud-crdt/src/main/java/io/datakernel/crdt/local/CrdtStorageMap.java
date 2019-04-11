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
import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.CrdtFunction;
import io.datakernel.crdt.CrdtStorage;
import io.datakernel.crdt.primitives.CrdtType;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.Initializable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

public final class CrdtStorageMap<K extends Comparable<K>, S> implements CrdtStorage<K, S>, Initializable<CrdtStorageMap<K, S>>, EventloopService, EventloopJmxMBeanEx {
	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final CrdtFunction<S> crdtFunction;

	private final SortedMap<K, CrdtData<K, S>> storage = new ConcurrentSkipListMap<>();
	private final Set<K> removed = new HashSet<>();

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();

	private final EventStats singlePuts = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleGets = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleRemoves = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private CrdtStorageMap(Eventloop eventloop, CrdtFunction<S> crdtFunction) {
		this.eventloop = eventloop;
		this.crdtFunction = crdtFunction;
	}

	public static <K extends Comparable<K>, S> CrdtStorageMap<K, S> create(Eventloop eventloop, CrdtFunction<S> crdtFunction) {
		return new CrdtStorageMap<>(eventloop, crdtFunction);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> CrdtStorageMap<K, S> create(Eventloop eventloop) {
		return new CrdtStorageMap<>(eventloop, CrdtFunction.<S>ofCrdtType());
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return Promise.of(StreamConsumer.of(this::doPut)
				.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
				.withLateBinding());
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return Promise.of(StreamSupplier.ofStream(extract(timestamp))
				.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
				.withLateBinding());
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Override
	public Promise<StreamConsumer<K>> remove() {
		return Promise.of(StreamConsumer.<K>of(
				key -> {
					storage.remove(key);
					removed.add(key);
				})
				.transformWith(detailedStats ? removeStatsDetailed : removeStats)
				.withLateBinding());
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

	private Stream<CrdtData<K, S>> extract(long timestamp) {
		Stream<CrdtData<K, S>> stream = storage.values().stream();
		if (timestamp == 0) {
			return stream;
		}
		return stream
				.map(data -> {
					S partial = crdtFunction.extract(data.getState(), timestamp);
					return partial != null ? new CrdtData<>(data.getKey(), partial) : null;
				})
				.filter(Objects::nonNull);
	}

	private void doPut(CrdtData<K, S> data) {
		K key = data.getKey();
		removed.remove(key);
		storage.merge(key, data, (a, b) -> new CrdtData<>(key, crdtFunction.merge(a.getState(), b.getState())));
	}

	public void put(K key, S state) {
		put(new CrdtData<>(key, state));
	}

	public void put(CrdtData<K, S> data) {
		singlePuts.recordEvent();
		doPut(data);
	}

	@Nullable
	public S get(K key) {
		singleGets.recordEvent();
		CrdtData<K, S> data = storage.get(key);
		return data != null ? data.getState() : null;
	}

	public boolean remove(K key) {
		singleRemoves.recordEvent();
		removed.add(key);
		return storage.remove(key) != null;
	}

	public Set<K> getRemoved() {
		return Collections.unmodifiableSet(removed);
	}

	public void cleanup() {
		removed.clear();
	}

	public Iterator<CrdtData<K, S>> iterator(long timestamp) {
		Iterator<CrdtData<K, S>> iterator = extract(timestamp).iterator();

		// had to hook the remove so it would be reflected in the storage
		return new Iterator<CrdtData<K, S>>() {
			private CrdtData<K, S> current;

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public CrdtData<K, S> next() {
				return current = iterator.next();
			}

			@Override
			public void remove() {
				if (current != null) {
					CrdtStorageMap.this.remove(current.getKey());
				}
				iterator.remove();
			}
		};
	}

	public Iterator<CrdtData<K, S>> iterator() {
		return iterator(0);
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
	public boolean isDetailedStats() {
		return detailedStats;
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
