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

import com.google.inject.name.Named;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.*;
import io.datakernel.util.guice.OptionalDependency;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofEventloopTaskScheduler;
import static io.datakernel.launchers.remotefs.Initializers.*;
import static io.datakernel.remotefs.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class RemoteFsClusterLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "remotefs-cluster.properties";

	@Inject
	RemoteFsRepartitionController controller;

	@Inject
	@Named("repartition")
	EventloopTaskScheduler repartitionScheduler;

	@Inject
	@Named("clusterDeadCheck")
	EventloopTaskScheduler clusterDeadCheckScheduler;

	@Override
	protected final Collection<Module> getModules() {
		return singletonList(override(getBaseModules()).with(getOverrideModules()));
	}

	private Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.override(Config.ofProperties(PROPERTIES_FILE, true))
								.override(Config.ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new AbstractModule() {
					@Provides
					@Singleton
					Eventloop provide(Config config, OptionalDependency<ThrottlingController> maybeThrottlingController) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withInspector));
					}

					@Provides
					@Singleton
					@Named("repartition")
					EventloopTaskScheduler repartitionScheduler(Config config, Eventloop eventloop, RemoteFsRepartitionController controller) {
						return EventloopTaskScheduler.create(eventloop, controller::repartition)
								.initialize(ofEventloopTaskScheduler(config.getChild("scheduler.repartition")));
					}

					@Provides
					@Singleton
					@Named("clusterDeadCheck")
					EventloopTaskScheduler deadCheckScheduler(Config config, Eventloop eventloop, RemoteFsClusterClient cluster) {
						return EventloopTaskScheduler.create(eventloop, cluster::checkDeadPartitions)
								.initialize(ofEventloopTaskScheduler(config.getChild("scheduler.cluster.deadCheck")));
					}

					@Provides
					@Singleton
					RemoteFsRepartitionController repartitionController(Config config,
							RemoteFsServer localServer, RemoteFsClusterClient cluster) {
						return RemoteFsRepartitionController.create(config.get("remotefs.repartition.localPartitionId"), cluster)
								.initialize(ofRepartitionController(config.getChild("remotefs.repartition")));
					}

					@Provides
					@Singleton
					RemoteFsClusterClient remoteFsClusterClient(Config config,
							RemoteFsServer localServer, Eventloop eventloop,
							OptionalDependency<ServerSelector> maybeServerSelector) {
						Map<Object, FsClient> clients = new HashMap<>();
						clients.put(config.get("remotefs.repartition.localPartitionId"), localServer.getClient());
						return RemoteFsClusterClient.create(eventloop, clients)
								.withServerSelector(maybeServerSelector.orElse(RENDEZVOUS_HASH_SHARDER))
								.initialize(ofRemoteFsCluster(eventloop, config.getChild("remotefs.cluster")));
					}

					@Provides
					@Singleton
					RemoteFsServer remoteFsServer(Config config, Eventloop eventloop, ExecutorService executor) {
						return RemoteFsServer.create(eventloop, executor, config.get(ofPath(), "remotefs.server.path"))
								.initialize(ofRemoteFsServer(config.getChild("remotefs.server")));
					}

					@Provides
					@Singleton
					public ExecutorService executorService() {
						return Executors.newSingleThreadExecutor();
					}
				}
		);
	}

	protected Collection<Module> getOverrideModules() {
		return emptyList();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RemoteFsClusterLauncher() {
		};
		launcher.launch(args);
	}
}
