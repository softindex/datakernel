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

package io.datakernel.launchers;

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.crdt.CrdtClusterClient;
import io.datakernel.crdt.RemoteCrdtClient;
import io.datakernel.crdt.local.FsCrdtClient;
import io.datakernel.crdt.local.RuntimeCrdtClient;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launchers.crdt.CrdtDescriptor;
import io.datakernel.util.Initializer;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.util.Preconditions.checkState;

public final class Initializers {
	private Initializers() {
		throw new AssertionError("nope.");
	}

	public static <T extends AbstractServer<T>> Initializer<T> ofAbstractServer(Config config) {
		return server -> server
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "listenAddresses"))
				.withAcceptOnce(config.get(ofBoolean(), "acceptOnce", false))
				.withSocketSettings(config.get(ofSocketSettings(), "socketSettings", server.getSocketSettings()))
				.withServerSocketSettings(config.get(ofServerSocketSettings(), "serverSocketSettings", server.getServerSocketSettings()));
	}

	public static Initializer<PrimaryServer> ofPrimaryServer(Config config) {
		return ofAbstractServer(config);
	}

	public static Initializer<Eventloop> ofEventloop(Config config) {
		return eventloop -> eventloop
				.withFatalErrorHandler(config.get(ofFatalErrorHandler(), "fatalErrorHandler", eventloop.getFatalErrorHandler()))
				.withIdleInterval(config.get(ofDuration(), "idleInterval", eventloop.getIdleInterval()))
				.withThreadPriority(config.get(ofInteger(), "threadPriority", eventloop.getThreadPriority()));
	}

	public static Initializer<EventloopTaskScheduler> ofEventloopTaskScheduler(Config config) {
		return scheduler -> {
			scheduler.setEnabled(!config.get(ofBoolean(), "disabled", false));
			scheduler.withAbortOnError(config.get(ofBoolean(), "abortOnError", false))
					.withInitialDelay(config.get(ofDuration(), "initialDelay", Duration.ZERO))
					.withSchedule(config.get(ofEventloopTaskSchedule(), "schedule", null))
					.withRetryPolicy(config.get(ofRetryPolicy(), "retryPolicy"));
		};
	}

	public static Initializer<AsyncHttpServer> ofHttpServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config))
				.initialize(ofHttpWorker(config));
	}

	public static Initializer<AsyncHttpServer> ofHttpWorker(Config config) {
		return server -> server
				.withKeepAliveTimeout(config.get(ofDuration(), "keepAliveTimeout", server.getKeepAliveTimeout()))
				.withReadWriteTimeout(config.get(ofDuration(), "readWriteTimeout", server.getReadWriteTimeout()));
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
