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

import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamSplitter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBeanEx;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;

public final class CrdtRepartitionController<I extends Comparable<I>, K extends Comparable<K>, S> implements EventloopJmxMBeanEx {
	private final I localPartitionId;
	private final CrdtStorage<K, S> localClient;
	private final CrdtStorageCluster<I, K, S> cluster;

	public CrdtRepartitionController(I localPartitionId, CrdtStorage<K, S> localClient, CrdtStorageCluster<I, K, S> cluster) {
		this.localClient = localClient;
		this.cluster = cluster;
		this.localPartitionId = localPartitionId;
	}

	public static <I extends Comparable<I>, K extends Comparable<K>, S> CrdtRepartitionController<I, K, S> create(CrdtStorageCluster<I, K, S> cluster, I localPartitionId) {
		return new CrdtRepartitionController<>(localPartitionId, cluster.getClients().get(localPartitionId), cluster);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return cluster.getEventloop();
	}

	public Promise<Void> repartition() {
		return Promises.toTuple(cluster.upload().toTry(), localClient.remove().toTry(), localClient.download().toTry())
				.then(all -> {
					if (all.getValue1().isSuccess() && all.getValue2().isSuccess() && all.getValue3().isSuccess()) {
						StreamConsumer<CrdtData<K, S>> cluster = all.getValue1().get();
						StreamConsumer<K> remover = all.getValue2().get();
						StreamSupplier<CrdtData<K, S>> downloader = all.getValue3().get();

						int index = this.cluster.getOrderedIds().indexOf(localPartitionId);

						StreamSplitter<CrdtData<K, S>, Object> splitter = StreamSplitter.create(
								(data, acceptors) -> {
									StreamDataAcceptor<Object> clusterAcceptor = acceptors[0];
									StreamDataAcceptor<Object> removeAcceptor = acceptors[1];
									clusterAcceptor.accept(data);
									int[] selected = this.cluster.getShardingFunction().shard(data.getKey());
									for (int s : selected) {
										if (s == index) {
											return;
										}
									}
									removeAcceptor.accept(data.getKey());
								});

						//noinspection unchecked, rawtypes
						((StreamSupplier) splitter.newOutput()).streamTo(cluster);
						//noinspection unchecked, rawtypes
						((StreamSupplier) splitter.newOutput()).streamTo(remover);

						return downloader.streamTo(splitter.getInput());
					} else {
						StacklessException exception = new StacklessException("Repartition exceptions:");
						all.getValue1().consume(AsyncCloseable::close, exception::addSuppressed);
						all.getValue2().consume(AsyncCloseable::close, exception::addSuppressed);
						all.getValue3().consume(AsyncCloseable::close, exception::addSuppressed);
						return Promise.ofException(exception);
					}
				});
	}
}
