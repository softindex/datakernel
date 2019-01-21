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

package io.datakernel.crdt;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.stream.processor.StreamMapSplitter;
import org.jetbrains.annotations.NotNull;

public final class CrdtRepartitionController<I extends Comparable<I>, K extends Comparable<K>, S> implements EventloopJmxMBeanEx {
	private final I localPartitionId;
	private final CrdtClient<K, S> localClient;
	private final CrdtClusterClient<I, K, S> cluster;

	public CrdtRepartitionController(I localPartitionId, CrdtClient<K, S> localClient, CrdtClusterClient<I, K, S> cluster) {
		this.localClient = localClient;
		this.cluster = cluster;
		this.localPartitionId = localPartitionId;
	}

	public static <I extends Comparable<I>, K extends Comparable<K>, S> CrdtRepartitionController<I, K, S> create(CrdtClusterClient<I, K, S> cluster, I localPartitionId) {
		return new CrdtRepartitionController<>(localPartitionId, cluster.getClients().get(localPartitionId), cluster);
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return cluster.getEventloop();
	}

	@SuppressWarnings("unchecked")
	public Promise<Void> repartition() {
		return Promises.toTuple(cluster.upload(), localClient.remove(), localClient.download().getStreamPromise())
				.thenCompose(tuple -> {
					int index = cluster.getOrderedIds().indexOf(localPartitionId);
					StreamMapSplitter<CrdtData<K, S>> forker = StreamMapSplitter.create((data, acceptors) -> {
						acceptors[0].accept(data);
						int[] selected = cluster.getShardingFunction().shard(data.getKey());
						for (int s : selected) {
							if (s == index) {
								return;
							}
						}
						acceptors[1].accept(data.getKey());
					});
					forker.<CrdtData<K, S>>newOutput().streamTo(tuple.getValue1());
					forker.<K>newOutput().streamTo(tuple.getValue2());
					return tuple.getValue3().streamTo(forker.getInput());
				});
	}
}
