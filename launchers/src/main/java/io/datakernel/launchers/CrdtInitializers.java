package io.datakernel.launchers;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.crdt.CrdtClusterClient;
import io.datakernel.crdt.RemoteCrdtClient;
import io.datakernel.crdt.local.FsCrdtClient;
import io.datakernel.crdt.local.RuntimeCrdtClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launchers.crdt.CrdtDescriptor;
import io.datakernel.util.Initializer;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.util.Preconditions.checkState;

public final class CrdtInitializers {
	private CrdtInitializers() {
	}

	public static <K extends Comparable<K>, S> Initializer<FsCrdtClient<K, S>> ofFsCrdtClient(Config config) {
		return fsCrdtClient ->
				fsCrdtClient.withConsolidationFolder(config.get("metafolder.consolidation", ".consolidation"))
						.withTombstoneFolder(config.get("metafolder.tombstones", ".tombstones"))
						.withConsolidationMargin(config.get(ofDuration(), "consolidationMargin", Duration.ofMinutes(30)));
	}

	public static <K extends Comparable<K>, S> Initializer<CrdtClusterClient<String, K, S>> ofCrdtCluster(
			Config config, RuntimeCrdtClient<K, S> localClient, CrdtDescriptor<K, S> descriptor) {
		return cluster -> {
			Eventloop eventloop = localClient.getEventloop();

			Map<String, Config> partitions = config.getChild("partitions").getChildren();
			checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");

			for (Map.Entry<String, Config> entry : partitions.entrySet()) {
				InetSocketAddress address = ConfigConverters.ofInetSocketAddress().get(entry.getValue());
				cluster.withPartition(entry.getKey(), RemoteCrdtClient.create(eventloop, address, descriptor.getSerializer()));
			}
			cluster.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
		};
	}

}
