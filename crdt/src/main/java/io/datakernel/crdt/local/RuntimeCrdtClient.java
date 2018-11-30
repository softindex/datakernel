/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.crdt.CrdtClient;
import io.datakernel.crdt.CrdtData;
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

import java.time.Duration;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

public final class RuntimeCrdtClient<K extends Comparable<K>, S> implements CrdtClient<K, S>, Initializable<RuntimeCrdtClient<K, S>>, EventloopService, EventloopJmxMBeanEx {
	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final BinaryOperator<StateWithTimestamp<S>> combiner;
	private final SortedMap<K, StateWithTimestamp<S>> storage;

	private final Set<K> removedKeys = new HashSet<>();
	private final Set<K> removedWhileDownloading = new HashSet<>();
	private int downloadCalls = 0;

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

	private RuntimeCrdtClient(Eventloop eventloop, BinaryOperator<S> combiner) {
		this.eventloop = eventloop;
		this.combiner = (a, b) -> new StateWithTimestamp<>(combiner.apply(a.state, b.state), Math.max(a.timestamp, b.timestamp));
		storage = new TreeMap<>();
	}

	public static <K extends Comparable<K>, V> RuntimeCrdtClient<K, V> create(Eventloop eventloop, BinaryOperator<V> combiner) {
		return new RuntimeCrdtClient<>(eventloop, combiner);
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return Promise.of(uploader());
	}

	@Override
	public StreamConsumer<CrdtData<K, S>> uploader() {
		long timestamp = eventloop.currentTimeMillis();
		return StreamConsumer.<CrdtData<K, S>>of(data -> {
			K key = data.getKey();
			removedKeys.remove(key);
			storage.merge(key, new StateWithTimestamp<>(data.getState(), timestamp), combiner);
		})
				.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
				.withLateBinding();
	}

	@Override
	public CrdtStreamSupplierWithToken<K, S> download(long token) {
		downloadCalls++;
		Stream<Map.Entry<K, StateWithTimestamp<S>>> stream = storage.entrySet().stream();
		StreamSupplier<CrdtData<K, S>> producer = StreamSupplier.ofStream(
				(token == 0 ? stream : stream.filter(entry -> entry.getValue().timestamp >= token))
						.map(entry -> new CrdtData<>(entry.getKey(), entry.getValue().state)))
				.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
				.withEndOfStream(eos -> eos.whenComplete(($, e) -> {
					downloadCalls--;
					removedWhileDownloading.forEach(storage::remove);
					removedWhileDownloading.clear();
				}))
				.withLateBinding();
		return new CrdtStreamSupplierWithToken<>(Promise.of(producer), Promise.of(eventloop.currentTimeMillis()));
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return Promise.of(remover());
	}

	@Override
	public StreamConsumer<K> remover() {
		return StreamConsumer.<K>of(key -> {
			if (downloadCalls > 0) {
				removedWhileDownloading.add(key);
			} else {
				storage.remove(key);
			}
			removedKeys.add(key);
		})
				.transformWith(detailedStats ? removeStatsDetailed : removeStats)
				.withLateBinding();
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete();
	}

	@Override
	public Promise<Void> start() {
		return Promise.complete();
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	public void put(K key, S state) {
		singlePuts.recordEvent();
		removedKeys.remove(key);
		storage.merge(key, new StateWithTimestamp<>(state, eventloop.currentTimeMillis()), combiner);
	}

	public void put(CrdtData<K, S> data) {
		put(data.getKey(), data.getState());
	}

	@Nullable
	public S get(K key) {
		singleGets.recordEvent();
		StateWithTimestamp<S> stateWithTimestamp = storage.get(key);
		return stateWithTimestamp != null ? stateWithTimestamp.state : null;
	}

	public boolean remove(K key) {
		singleRemoves.recordEvent();
		removedKeys.add(key);
		return storage.remove(key) != null;
	}

	public Set<K> getRemovedKeys() {
		return Collections.unmodifiableSet(removedKeys);
	}

	public void clearRemovedKeys() {
		removedKeys.clear();
	}

	public Iterator<CrdtData<K, S>> iterator(long timestamp) {
		Stream<Map.Entry<K, StateWithTimestamp<S>>> stream = storage.entrySet().stream();

		Iterator<CrdtData<K, S>> iterator =
				(timestamp == 0 ? stream : stream.filter(e -> e.getValue().timestamp >= timestamp))
						.map(e -> new CrdtData<>(e.getKey(), e.getValue().state))
						.collect(toCollection(LinkedList::new))
						.iterator();

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
					storage.remove(current.getKey());
				}
				iterator.remove();
			}
		};
	}

	public Iterator<CrdtData<K, S>> iterator() {
		return iterator(0);
	}

	static class StateWithTimestamp<S> {
		final S state;
		final long timestamp;

		StateWithTimestamp(S state, long timestamp) {
			this.state = state;
			this.timestamp = timestamp;
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
