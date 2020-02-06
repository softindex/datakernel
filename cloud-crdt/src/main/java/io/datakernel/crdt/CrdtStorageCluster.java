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

package io.datakernel.crdt;

import io.datakernel.async.service.EventloopService;
import io.datakernel.common.Initializable;
import io.datakernel.common.collection.Try;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.crdt.primitives.CrdtType;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.MultiSharder;
import io.datakernel.datastream.processor.ShardingStreamSplitter;
import io.datakernel.datastream.processor.StreamReducerSimple;
import io.datakernel.datastream.processor.StreamReducers.BinaryAccumulatorReducer;
import io.datakernel.datastream.processor.StreamSplitter;
import io.datakernel.datastream.stats.StreamStats;
import io.datakernel.datastream.stats.StreamStatsBasic;
import io.datakernel.datastream.stats.StreamStatsDetailed;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.api.JmxOperation;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

import static io.datakernel.async.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toList;

public final class CrdtStorageCluster<I extends Comparable<I>, K extends Comparable<K>, S> implements CrdtStorage<K, S>, Initializable<CrdtStorageCluster<I, K, S>>, EventloopService, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CrdtStorageCluster.class);

	private final Eventloop eventloop;
	private final Map<I, CrdtStorage<K, S>> clients;
	private final Map<I, CrdtStorage<K, S>> aliveClients;
	private final Map<I, CrdtStorage<K, S>> deadClients;

	private final CrdtFunction<S> function;
	private final RendezvousHashSharder<I, K> shardingFunction;

	private List<I> orderedIds;

	private int replicationCount = 1;
	private CrdtFilter<S> filter = $ -> true;

	// region JMX
	private boolean detailedStats;

	private final StreamStatsBasic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStatsDetailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStatsBasic<K> removeStats = StreamStats.basic();
	private final StreamStatsDetailed<K> removeStatsDetailed = StreamStats.detailed();
	// endregion

	// region creators
	private CrdtStorageCluster(Eventloop eventloop, Map<I, CrdtStorage<K, S>> clients, CrdtFunction<S> function) {
		this.eventloop = eventloop;
		this.clients = clients;
		this.aliveClients = new LinkedHashMap<>(clients); // to keep order for indexed sharding
		this.deadClients = new HashMap<>();
		this.function = function;
		shardingFunction = RendezvousHashSharder.create(orderedIds = new ArrayList<>(aliveClients.keySet()), replicationCount);
	}

	public static <I extends Comparable<I>, K extends Comparable<K>, S> CrdtStorageCluster<I, K, S> create(
			Eventloop eventloop, Map<I, ? extends CrdtStorage<K, S>> clients, CrdtFunction<S> crdtFunction
	) {
		return new CrdtStorageCluster<>(eventloop, new HashMap<>(clients), crdtFunction);
	}

	public static <I extends Comparable<I>, K extends Comparable<K>, S extends CrdtType<S>> CrdtStorageCluster<I, K, S> create(
			Eventloop eventloop, Map<I, ? extends CrdtStorage<K, S>> clients
	) {
		return new CrdtStorageCluster<>(eventloop, new HashMap<>(clients), CrdtFunction.ofCrdtType());
	}

	public CrdtStorageCluster<I, K, S> withPartition(I partitionId, CrdtStorage<K, S> client) {
		clients.put(partitionId, client);
		aliveClients.put(partitionId, client);
		recompute();
		return this;
	}

	public CrdtStorageCluster<I, K, S> withReplicationCount(int replicationCount) {
		this.replicationCount = replicationCount;
		recompute();
		return this;
	}

	public CrdtStorageCluster<I, K, S> withFilter(CrdtFilter<S> filter) {
		this.filter = filter;
		return this;
	}
	// endregion

	// region getters
	public Map<I, ? extends CrdtStorage<K, S>> getClients() {
		return Collections.unmodifiableMap(clients);
	}

	public Map<I, CrdtStorage<K, S>> getAliveClients() {
		return Collections.unmodifiableMap(aliveClients);
	}

	public Map<I, CrdtStorage<K, S>> getDeadClients() {
		return Collections.unmodifiableMap(deadClients);
	}

	public List<I> getOrderedIds() {
		return Collections.unmodifiableList(orderedIds);
	}

	public MultiSharder<K> getShardingFunction() {
		return shardingFunction;
	}
	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	public Promise<Void> checkAllPartitions() {
		return Promises.all(clients.entrySet().stream()
				.map(entry -> {
					I id = entry.getKey();
					return entry.getValue()
							.ping()
							.mapEx(($, e) -> {
								if (e == null) {
									markAlive(id);
								} else {
									markDead(id, e);
								}
								return null;
							});
				}))
				.whenComplete(toLogger(logger, "checkAllPartitions"));
	}

	public Promise<Void> checkDeadPartitions() {
		return Promises.all(deadClients.entrySet().stream()
				.map(e -> e.getValue()
						.ping()
						.mapEx(($, exc) -> {
							if (exc == null) {
								markAlive(e.getKey());
							}
							return null;
						})))
				.whenComplete(toLogger(logger, "checkDeadPartitions"));
	}

	private void markAlive(I partitionId) {
		CrdtStorage<K, S> removed = deadClients.remove(partitionId);
		if (removed != null) {
			aliveClients.put(partitionId, removed);
			recompute();
			logger.info("Marked partition {} as alive", partitionId);
		}
	}

	public void markDead(I partitionId, Throwable err) {
		CrdtStorage<K, S> removed = aliveClients.remove(partitionId);
		if (removed != null) {
			deadClients.put(partitionId, removed);
			recompute();
			logger.warn("Marked partition {} as dead", partitionId, err);
		}
	}

	private void recompute() {
		shardingFunction.recompute(orderedIds = new ArrayList<>(aliveClients.keySet()), replicationCount);
	}

	private <T> Promise<List<T>> connect(Function<CrdtStorage<K, S>, Promise<T>> method) {
		return Promises.toList(
				aliveClients.entrySet().stream()
						.map(entry ->
								method.apply(entry.getValue())
										.whenException(err -> markDead(entry.getKey(), err))
										.toTry()))
				.then(tries -> {
					List<T> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());
					if (successes.isEmpty()) {
						return Promise.ofException(new StacklessException(CrdtStorageCluster.class, "No successful connections"));
					}
					return Promise.of(successes);
				});
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return connect(CrdtStorage::upload)
				.then(successes -> {
					ShardingStreamSplitter<CrdtData<K, S>, K> splitter = ShardingStreamSplitter.create(shardingFunction, CrdtData::getKey);

					successes.forEach(consumer -> splitter.newOutput().streamTo(consumer));

					return Promise.of(splitter.getInput()
							.transformWith(detailedStats ? uploadStats : uploadStatsDetailed)
							.withLateBinding());
				});
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return connect(storage -> storage.download(timestamp))
				.then(successes -> {
					StreamReducerSimple<K, CrdtData<K, S>, CrdtData<K, S>, CrdtData<K, S>> reducer =
							StreamReducerSimple.create(CrdtData::getKey, Comparator.naturalOrder(),
									new BinaryAccumulatorReducer<K, CrdtData<K, S>>((a, b) -> new CrdtData<>(a.getKey(), function.merge(a.getState(), b.getState())))
											.withFilter(data -> filter.test(data.getState())));

					successes.forEach(producer -> producer.streamTo(reducer.newInput()));

					return Promise.of(reducer.getOutput()
							.transformWith(detailedStats ? downloadStats : downloadStatsDetailed));
				});
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return connect(CrdtStorage::remove)
				.then(successes -> {
					StreamSplitter<K> splitter = StreamSplitter.create();

					successes.forEach(consumer -> splitter.newOutput().streamTo(consumer));

					return Promise.of(splitter.getInput()
							.transformWith(detailedStats ? removeStats : removeStatsDetailed));
				});
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete();  // Promises.all(aliveClients.values().stream().map(CrdtClient::ping));
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

	// region JMX
	@JmxAttribute
	public int getReplicationCount() {
		return replicationCount;
	}

	@JmxAttribute
	public void setReplicationCount(int replicationCount) {
		withReplicationCount(replicationCount);
	}

	@JmxAttribute
	public int getAlivePartitionCount() {
		return aliveClients.size();
	}

	@JmxAttribute
	public int getDeadPartitionCount() {
		return deadClients.size();
	}

	@JmxAttribute
	public String[] getAlivePartitions() {
		return aliveClients.keySet().stream()
				.map(Object::toString)
				.toArray(String[]::new);
	}

	@JmxAttribute
	public String[] getDeadPartitions() {
		return deadClients.keySet().stream()
				.map(Object::toString)
				.toArray(String[]::new);
	}

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
	// endregion
}
