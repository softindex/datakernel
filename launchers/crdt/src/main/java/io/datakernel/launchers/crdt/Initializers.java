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

package io.datakernel.launchers.crdt;

import io.datakernel.common.Initializer;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.crdt.remote.CrdtRemoteClient;
import io.datakernel.crdt.CrdtCluster;
import io.datakernel.crdt.local.CrdtClientFs;
import io.datakernel.crdt.local.CrdtClientMap;
import io.datakernel.eventloop.Eventloop;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofInteger;

public final class Initializers {

	public static <K extends Comparable<K>, S> Initializer<CrdtClientFs<K, S>> ofFsCrdtClient(Config config) {
		return fsCrdtClient ->
				fsCrdtClient.withConsolidationFolder(config.get("metafolder.consolidation", ".consolidation"))
						.withTombstoneFolder(config.get("metafolder.tombstones", ".tombstones"))
						.withConsolidationMargin(config.get(ofDuration(), "consolidationMargin", Duration.ofMinutes(30)));
	}

	public static <K extends Comparable<K>, S> Initializer<CrdtCluster<String, K, S>> ofCrdtCluster(
			Config config, CrdtClientMap<K, S> localClient, CrdtDescriptor<K, S> descriptor) {
		return cluster -> {
			Eventloop eventloop = localClient.getEventloop();

			Map<String, Config> partitions = config.getChild("partitions").getChildren();
			checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");

			for (Map.Entry<String, Config> entry : partitions.entrySet()) {
				InetSocketAddress address = ConfigConverters.ofInetSocketAddress().get(entry.getValue());
				cluster.withPartition(entry.getKey(), CrdtRemoteClient.create(eventloop, address, descriptor.getSerializer()));
			}
			cluster.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
		};
	}

}
