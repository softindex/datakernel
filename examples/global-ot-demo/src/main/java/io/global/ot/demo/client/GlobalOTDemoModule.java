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

package io.global.ot.demo.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRepository;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.demo.api.OTStateServlet;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.demo.util.ManagerProvider;
import io.global.ot.http.GlobalOTNodeHttpClient;
import io.global.ot.util.Bootstrap;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.ot.GlobalOTConfigConverters.ofMyRepositoryId;
import static io.global.ot.demo.util.Utils.OPERATION_CODEC;
import static io.global.ot.demo.util.Utils.createOTSystem;
import static java.util.Collections.emptySet;
import static java.util.concurrent.Executors.newCachedThreadPool;

final class GlobalOTDemoModule extends AbstractModule {
	private static final String DEFAULT_PATH_TO_RESOURCES = "src/main/resources/static";
	private static final PrivKey DEMO_PRIVATE_KEY =
			PrivKey.of(new BigInteger("52a8fbf6c82e3e177a07d5fb822bbef07c1f28cfaeeb320964a4598ea82159b", 16));
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	private static final RepoID DEMO_REPO_ID = RepoID.of(DEMO_PRIVATE_KEY.computePubKey(), "Example");
	private static final MyRepositoryId<Operation> DEMO_MY_REPOSITORY_ID = new MyRepositoryId<>(DEMO_REPO_ID, DEMO_PRIVATE_KEY, OPERATION_CODEC);
	private static final String DEMO_NODE_ADDRESS = "http://127.0.0.1:9000/ot/";

	@Override
	protected void configure() {
		bind(ExecutorService.class).toInstance(newCachedThreadPool());
	}

	@Provides
	@Singleton
	Eventloop provide() {
		return Eventloop.create();
	}

	@Provides
	@Singleton
	AsyncHttpServer provide(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	AsyncServlet provideMainServlet(Eventloop eventloop, ManagerProvider<Operation> managerProvider, Config config) {
		Path resources = Paths.get(DEFAULT_PATH_TO_RESOURCES);
		StaticLoader resourceLoader = StaticLoaders.ofPath(newCachedThreadPool(), resources);
		StaticServlet staticServlet = StaticServlet.create(eventloop, resourceLoader);
		return OTStateServlet.create(managerProvider).getMiddlewareServlet()
				.withFallback(staticServlet);
	}

	@Provides
	@Singleton
	ManagerProvider<Operation> provideManager(OTAlgorithms<CommitId, Operation> algorithms) {
		return new ManagerProvider<>(algorithms, OperationState::new);
	}

	@Provides
	@Singleton
	OTRepository<CommitId, Operation> provideRepository(Bootstrap<Operation> bootstrap, MyRepositoryId<Operation> myRepositoryId) {
		return new OTRepositoryAdapter<>(bootstrap.getDriver(), myRepositoryId, emptySet());
	}

	@Provides
	@Singleton
	GlobalOTNode provideGlobalOTNode(Eventloop eventloop, Config config) {
		return GlobalOTNodeHttpClient.create(AsyncHttpClient.create(eventloop), config.get("node.serverId", DEMO_NODE_ADDRESS));
	}

	@Provides
	@Singleton
	OTDriver provideDriver(GlobalOTNode globalOTNode, Config config) {
		return new OTDriver(globalOTNode, config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY));
	}

	@Provides
	@Singleton
	MyRepositoryId<Operation> provideMyRepositoryId(Config config) {
		return config.get(ofMyRepositoryId(OPERATION_CODEC), "credentials", DEMO_MY_REPOSITORY_ID);
	}

	@Provides
	@Singleton
	Bootstrap<Operation> provideBootstrap(Eventloop eventloop, OTDriver driver, MyRepositoryId<Operation> myRepositoryId) {
		return new Bootstrap<>(eventloop, driver, myRepositoryId);
	}

	@Provides
	@Singleton
	OTAlgorithms<CommitId, Operation> provide(Eventloop eventloop, OTRepository<CommitId, Operation> repository) {
		return OTAlgorithms.create(eventloop, createOTSystem(), repository);
	}
}
