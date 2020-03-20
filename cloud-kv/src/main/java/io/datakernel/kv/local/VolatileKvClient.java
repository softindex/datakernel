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
import io.datakernel.common.Initializable;
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
import io.datakernel.kv.KvItem;
import io.datakernel.kv.KvClient;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

public final class VolatileKvClient<K, V> implements KvClient<K, V>, Initializable<VolatileKvClient<K, V>>, EventloopService, EventloopJmxMBeanEx {
	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;

	private final Map<String, Map<K, KvItem<K, V>>> storage = new HashMap<>();

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<KvItem<K, V>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<KvItem<K, V>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<KvItem<K, V>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<KvItem<K, V>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();

	private final EventStats singlePuts = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleGets = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleRemoves = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private VolatileKvClient(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static <K extends Comparable<K>, S> VolatileKvClient<K, S> create(Eventloop eventloop) {
		return new VolatileKvClient<>(eventloop);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Override
	public Promise<StreamConsumer<KvItem<K, V>>> upload(String table) {
		return Promise.of(StreamConsumer.<KvItem<K, V>>of(item -> doPut(table, item))
				.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
				.withLateBinding());
	}

	@Override
	public Promise<StreamSupplier<KvItem<K, V>>> download(String table, long revision) {
		return Promise.of(StreamSupplier.ofStream(extract(table, revision))
				.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
				.withLateBinding());
	}

	@SuppressWarnings("deprecation") // StreamConsumer#of
	@Override
	public Promise<StreamConsumer<K>> remove(String table) {
		return Promise.of(StreamConsumer.<K>of(storage.getOrDefault(table, emptyMap())::remove)
				.transformWith(detailedStats ? removeStatsDetailed : removeStats)
				.withLateBinding());
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

	private Stream<KvItem<K, V>> extract(String table, long revision) {
		Stream<KvItem<K, V>> stream = storage.getOrDefault(table, emptyMap()).values().stream();
		return revision != 0 ?
				stream.filter(data -> data.getTimestamp() >= revision) :
				stream;
	}

	private void doPut(String table, KvItem<K, V> data) {
		storage
				.computeIfAbsent(table, $ -> new HashMap<>())
				.merge(data.getKey(), data, (a, b) -> a.getTimestamp() <= b.getTimestamp() ? b : a);
	}

	@Override
	public Promise<Void> put(String table, KvItem<K, V> data) {
		singlePuts.recordEvent();
		doPut(table, data);
		return Promise.complete();
	}

	@Override
	public Promise<@Nullable KvItem<K, V>> get(String table, K key) {
		singleGets.recordEvent();
		return Promise.of(storage.getOrDefault(table, emptyMap()).get(key));
	}

	@Override
	public Promise<Void> remove(String table, K key) {
		singleRemoves.recordEvent();
		storage.getOrDefault(table, emptyMap()).remove(key);
		return Promise.complete();
	}

	@Override
	public Promise<Set<String>> list() {
		return Promise.of(storage.keySet());
	}

	public Iterator<KvItem<K, V>> iterator(String table, long revision) {
		Iterator<KvItem<K, V>> iterator = extract(table, revision).iterator();
		return new Iterator<KvItem<K, V>>() {
			private KvItem<K, V> current;

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public KvItem<K, V> next() {
				return current = iterator.next();
			}

			@Override
			public void remove() {
				if (current != null) {
					VolatileKvClient.this.remove(table, current.getKey());
				}
			}
		};
	}

	public Iterator<KvItem<K, V>> iterator(String table) {
		return iterator(table, 0);
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
