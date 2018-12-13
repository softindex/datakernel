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

package io.datakernel.launchers.remotefs;

import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.remotefs.RemoteFsClusterClient;
import io.datakernel.remotefs.RemoteFsRepartitionController;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.util.Initializer;

import java.util.Map;

import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.launchers.initializers.Initializers.ofAbstractServer;
import static io.datakernel.util.Preconditions.checkState;

public final class Initializers {
	private Initializers() {}

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
			for (Map.Entry<String, Config> connection : partitions.entrySet()) {
				cluster.withPartition(connection.getKey(), RemoteFsClient.create(eventloop, connection.getValue().get(ofInetSocketAddress(), THIS)));
			}
			cluster.withReplicationCount(config.get(ofInteger(), "replicationCount", 1));
		};
	}

}
