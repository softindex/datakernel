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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.StacklessException;
import io.datakernel.functional.Try;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.MultiSharder;
import io.datakernel.stream.processor.ShardingStreamSplitter;
import io.datakernel.stream.processor.StreamReducerSimple;
import io.datakernel.stream.processor.StreamReducers.BinaryAccumulatorReducer;
import io.datakernel.stream.processor.StreamSplitter;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsBasic;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.util.Initializable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;

import static io.datakernel.util.LogUtils.toLogger;
import static java.util.stream.Collectors.toList;

public final class CrdtClusterClient<I extends Comparable<I>, K extends Comparable<K>, S> implements CrdtClient<K, S>, Initializable<CrdtClusterClient<I, K, S>>, EventloopService, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CrdtClusterClient.class);

	private final Comparator<Entry<I, CrdtClient<K, S>>> comparingByKey = Comparator.comparing(Entry::getKey);

	private final Eventloop eventloop;
	private final Map<I, CrdtClient<K, S>> clients;
	private final Map<I, CrdtClient<K, S>> aliveClients;
	private final Map<I, CrdtClient<K, S>> deadClients;

	private final BinaryOperator<CrdtData<K, S>> combiner;
	private final RendezvousHashSharder<I, K> shardingFunction;

	private List<I> orderedIds;

	private int replicationCount = 1;

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
	private CrdtClusterClient(Eventloop eventloop, Map<I, CrdtClient<K, S>> clients, BinaryOperator<S> combiner) {
		this.eventloop = eventloop;
		this.clients = clients;
		this.aliveClients = new LinkedHashMap<>(clients); // to keep order for indexed sharding
		this.deadClients = new HashMap<>();
		this.combiner = (a, b) -> new CrdtData<>(a.getKey(), combiner.apply(a.getState(), b.getState()));
		shardingFunction = RendezvousHashSharder.create(orderedIds = new ArrayList<>(aliveClients.keySet()), replicationCount);
	}

	public static <I extends Comparable<I>, K extends Comparable<K>, S> CrdtClusterClient<I, K, S> create(
			Eventloop eventloop, Map<I, ? extends CrdtClient<K, S>> clients, BinaryOperator<S> combiner
	) {
		return new CrdtClusterClient<>(eventloop, new HashMap<>(clients), combiner);
	}

	public CrdtClusterClient<I, K, S> withPartition(I partitionId, CrdtClient<K, S> client) {
		clients.put(partitionId, client);
		aliveClients.put(partitionId, client);
		recompute();
		return this;
	}

	public CrdtClusterClient<I, K, S> withReplicationCount(int replicationCount) {
		this.replicationCount = replicationCount;
		recompute();
		return this;
	}
	// endregion

	// region getters
	public Map<I, ? extends CrdtClient<K, S>> getClients() {
		return Collections.unmodifiableMap(clients);
	}

	public Map<I, CrdtClient<K, S>> getAliveClients() {
		return Collections.unmodifiableMap(aliveClients);
	}

	public Map<I, CrdtClient<K, S>> getDeadClients() {
		return Collections.unmodifiableMap(deadClients);
	}

	public List<I> getOrderedIds() {
		return Collections.unmodifiableList(orderedIds);
	}

	public MultiSharder<K> getShardingFunction() {
		return shardingFunction;
	}

	public BinaryOperator<CrdtData<K, S>> getCombiner() {
		return combiner;
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
							.thenApplyEx(($, e) -> {
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
						.thenApplyEx(($, exc) -> {
							if (exc == null) {
								markAlive(e.getKey());
							}
							return null;
						})))
				.whenComplete(toLogger(logger, "checkDeadPartitions"));
	}

	private void markAlive(I partitionId) {
		CrdtClient<K, S> removed = deadClients.remove(partitionId);
		if (removed != null) {
			aliveClients.put(partitionId, removed);
			recompute();
			logger.info("Marked partition {} as alive", partitionId);
		}
	}

	public void markDead(I partitionId, Throwable err) {
		CrdtClient<K, S> removed = aliveClients.remove(partitionId);
		if (removed != null) {
			deadClients.put(partitionId, removed);
			recompute();
			logger.warn("Marked partition {} as dead", partitionId, err);
		}
	}

	private void recompute() {
		shardingFunction.recompute(orderedIds = new ArrayList<>(aliveClients.keySet()), replicationCount);
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		return Promises.toList(
				aliveClients.entrySet().stream()
						.map(entry -> entry.getValue().upload()
								.whenException(err -> markDead(entry.getKey(), err))
								.toTry()))
				.thenCompose(tries -> {
					boolean[] anyConnection = {false};
					List<StreamConsumer<CrdtData<K, S>>> successes = tries.stream()
							.map(t -> {
								anyConnection[0] |= t.isSuccess();
								return t.getOrSupply(StreamConsumer::idle);
							})
							.collect(toList());
					if (!anyConnection[0]) {
						return Promise.ofException(new StacklessException(CrdtClusterClient.class, "No successful connections"));
					}
					ShardingStreamSplitter<CrdtData<K, S>, K> shplitter = ShardingStreamSplitter.create(shardingFunction, CrdtData::getKey);
					successes.forEach(consumer -> shplitter.newOutput().streamTo(consumer));
					return Promise.of(shplitter.getInput()
							.transformWith(detailedStats ? uploadStats : uploadStatsDetailed)
							.withLateBinding());
				});
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		return Promises.toList(
				aliveClients.entrySet().stream()
						.map(entry ->
								entry.getValue().download(timestamp)
										.whenException(err -> markDead(entry.getKey(), err))
										.toTry()))
				.thenCompose(tries -> {
					List<StreamSupplier<CrdtData<K, S>>> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());
					if (successes.isEmpty()) {
						return Promise.ofException(new StacklessException(CrdtClusterClient.class, "No successful connections"));
					}

					StreamReducerSimple<K, CrdtData<K, S>, CrdtData<K, S>, CrdtData<K, S>> reducer =
							StreamReducerSimple.create(CrdtData::getKey, Comparator.<K>naturalOrder(), new BinaryAccumulatorReducer<>(combiner));

					return Promise.of(StreamSupplier.<CrdtData<K, S>>ofConsumer(consumer ->
							reducer.getOutput()
									.transformWith(detailedStats ? downloadStats : downloadStatsDetailed)
									.streamTo(consumer
											.withAcknowledgement(ack -> ack.both(Promises.all(successes.stream()
													.map(producer -> producer.streamTo(reducer.newInput())))
													.materialize()))))
							.withLateBinding());
				});
	}

	@Override
	public Promise<StreamConsumer<K>> remove() {
		return Promises.toList(
				aliveClients.entrySet().stream()
						.sorted(comparingByKey)
						.map(entry -> entry.getValue().remove()
								.whenException(err -> markDead(entry.getKey(), err))
								.toTry()))
				.thenCompose(tries -> {
					List<StreamConsumer<K>> successes = tries.stream()
							.filter(Try::isSuccess)
							.map(Try::get)
							.collect(toList());
					if (successes.isEmpty()) {
						return Promise.ofException(new StacklessException(CrdtClusterClient.class, "No successful connections"));
					}
					StreamSplitter<K> splitter = StreamSplitter.create();
					successes.forEach(consumer -> splitter.newOutput().streamTo(consumer));
					return Promise.of(splitter.getInput()
							.transformWith(detailedStats ? removeStats : removeStatsDetailed)
							.withLateBinding());
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
