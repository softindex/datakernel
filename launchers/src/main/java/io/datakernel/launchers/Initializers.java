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
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.remotefs.RemoteFsClusterClient;
import io.datakernel.remotefs.RemoteFsRepartitionController;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.util.Initializer;
import io.global.fs.local.LocalGlobalFsNode;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.launchers.globalfs.GlobalFsConfigConverters.ofPubKey;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_INITIAL_BUFFER_SIZE;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_MAX_MESSAGE_SIZE;
import static io.datakernel.util.Preconditions.checkState;
import static io.global.fs.local.LocalGlobalFsNode.DEFAULT_LATENCY_MARGIN;
import static java.util.Collections.emptyList;

public class Initializers {
	private Initializers() {
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

	public static Initializer<RemoteFsServer> ofRemoteFsServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config));
	}

	public static Initializer<RemoteFsRepartitionController> ofRepartitionController(Config config) {
		return controller -> controller
				.withGlob(config.get("glob", "**"))
				.withNegativeGlob(config.get("negativeGlob", ""));
	}

	public static Initializer<RemoteFsClusterClient> ofRemoteFsCluster(Eventloop eventloop, Config config) {
		return cluster -> {
			Map<String, Config> partitions = config.getChild("partitions").getChildren();
			checkState(!partitions.isEmpty(), "Cluster could not operate without partitions, config had none");
			for (Entry<String, Config> connection : partitions.entrySet()) {
				cluster.withPartition(connection.getKey(), RemoteFsClient.create(eventloop, connection.getValue().get(ofInetSocketAddress(), THIS)));
			}
			cluster.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
		};
	}

	public static Initializer<RpcServer> ofRpcServer(Config config) {
		return server -> server
				.initialize(ofAbstractServer(config.getChild("rpc.server")))
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_INITIAL_BUFFER_SIZE),
						config.get(ofMemSize(), "rpc.streamProtocol.maxPacketSize", DEFAULT_MAX_MESSAGE_SIZE),
						config.get(ofBoolean(), "rpc.streamProtocol.compression", false))
				.withAutoFlushInterval(config.get(ofDuration(), "rpc.flushDelay", Duration.ZERO));
	}

	public static Initializer<LocalGlobalFsNode> ofLocalGlobalFsNode(Config config) {
		return node -> node
				.withManagedPubKeys(new HashSet<>(config.get(ofList(ofPubKey()), "managedKeys", emptyList())))
				.withDownloadCaching(config.get(ofBoolean(), "enableDownloadCaching"))
				.withUploadCaching(config.get(ofBoolean(), "enableUploadCaching"))
				.withLatencyMargin(config.get(ofDuration(), "latencyMargin", DEFAULT_LATENCY_MARGIN));
	}
}
