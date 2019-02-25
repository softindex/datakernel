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

package io.global.ot.editor.server;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.CachedAsyncDnsClient;
import io.datakernel.dns.DnsCache;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.common.DelayedPushNode;
import io.global.ot.editor.operations.EditorOperation;
import io.global.ot.graph.OTGraphServlet;
import io.global.ot.http.GlobalOTNodeHttpClient;
import io.global.ot.http.OTNodeServlet;
import io.global.ot.util.Bootstrap;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.dns.RemoteAsyncDnsClient.DEFAULT_TIMEOUT;
import static io.datakernel.dns.RemoteAsyncDnsClient.GOOGLE_PUBLIC_DNS;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.ConfigConverters.ofDnsCache;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.ot.GlobalOTConfigConverters.ofMyRepositoryId;
import static io.global.ot.editor.operations.EditorOTSystem.createOTSystem;
import static io.global.ot.editor.operations.Utils.ID_TO_STRING;
import static io.global.ot.editor.operations.Utils.OPERATION_CODEC;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.emptySet;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class GlobalEditorModule extends AbstractModule {
	private static final PrivKey DEMO_PRIVATE_KEY =
			PrivKey.of(new BigInteger("52a8fbf6c82e3e177a07d5fb822bbef07c1f28cfaeeb320964a4598ea82159b", 16));

	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	private static final RepoID DEMO_REPO_ID = RepoID.of(DEMO_PRIVATE_KEY.computePubKey(), "Editor Example");
	private static final MyRepositoryId<EditorOperation> DEMO_MY_REPOSITORY_ID = new MyRepositoryId<>(DEMO_REPO_ID, DEMO_PRIVATE_KEY, OPERATION_CODEC);
	private static final String DEMO_NODE_ADDRESS = "http://127.0.0.1:9000/ot/";
	private static final Path DEFAULT_RESOURCES_PATH = Paths.get("src/main/resources/static");
	private static final Duration DEFAULT_PUSH_DELAY_DURATION = Duration.ZERO;

	@Provides
	@Singleton
	Eventloop provideEventloop(Config config) {
		return Eventloop.create()
				.initialize(ofEventloop(config));
	}

	@Provides
	@Singleton
	AsyncHttpServer provideServer(Eventloop eventloop, MiddlewareServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	IAsyncHttpClient provideClient(Eventloop eventloop, AsyncDnsClient dnsClient) {
		return AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient);
	}

	@Provides
	@Singleton
	AsyncDnsClient provideDnsClient(Eventloop eventloop, Config config) {
		RemoteAsyncDnsClient remoteDnsClient = RemoteAsyncDnsClient.create(eventloop)
				.withDnsServerAddress(config.get(ofInetSocketAddress(), "dns.serverAddress", GOOGLE_PUBLIC_DNS))
				.withTimeout(config.get(ofDuration(), "dns.timeout", DEFAULT_TIMEOUT));
		return CachedAsyncDnsClient.create(eventloop, remoteDnsClient, config.get(ofDnsCache(eventloop), "dns.cache", DnsCache.create(eventloop)));
	}

	@Provides
	@Singleton
	MiddlewareServlet provideMiddlewareServlet(StaticServlet staticServlet, OTGraphServlet<CommitId, EditorOperation> graphServlet, OTNodeServlet<CommitId, EditorOperation, OTCommit<CommitId, EditorOperation>> nodeServlet) {
		return MiddlewareServlet.create()
				.with(GET, "/graph", graphServlet)
				.with("/node", nodeServlet)
				.withFallback(staticServlet);
	}

	@Provides
	@Singleton
	OTGraphServlet<CommitId, EditorOperation> provideGraphServlet(OTAlgorithms<CommitId, EditorOperation> algorithms) {
		return OTGraphServlet.create(algorithms, ID_TO_STRING, Object::toString)
				.withCurrentCommit(request -> {
					try {
						return Promise.of(fromJson(REGISTRY.get(CommitId.class), request.getQueryParameter("id")));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Provides
	@Singleton
	StaticServlet provideStaticServlet(Eventloop eventloop, Config config) {
		StaticLoader resourceLoader = StaticLoaders.ofPath(newCachedThreadPool(), config.get(ofPath(), "resources.path", DEFAULT_RESOURCES_PATH));
		return StaticServlet.create(eventloop, resourceLoader);
	}

	@Provides
	@Singleton
	OTNodeServlet<CommitId, EditorOperation, OTCommit<CommitId, EditorOperation>> provideNodeServlet(OTAlgorithms<CommitId, EditorOperation> algorithms, OTRepositoryAdapter<EditorOperation> repositoryAdapter, Config config) {
		Duration delay = config.get(ofDuration(), "push.delay", DEFAULT_PUSH_DELAY_DURATION);
		return OTNodeServlet.forGlobalNode(DelayedPushNode.create(algorithms.getOtNode(), delay), OPERATION_CODEC, repositoryAdapter);
	}

	@Provides
	@Singleton
	Bootstrap<EditorOperation> provideBootstrap(Eventloop eventloop, OTDriver driver, MyRepositoryId<EditorOperation> myRepositoryId) {
		return new Bootstrap<>(eventloop, driver, myRepositoryId);
	}

	@Provides
	@Singleton
	OTAlgorithms<CommitId, EditorOperation> provideAlgorithms(Eventloop eventloop, OTRepositoryAdapter<EditorOperation> repository) {
		return OTAlgorithms.create(eventloop, createOTSystem(), repository);
	}

	@Provides
	@Singleton
	OTRepositoryAdapter<EditorOperation> provideRepository(Eventloop eventloop, Bootstrap<EditorOperation> bootstrap) {
		return new OTRepositoryAdapter<>(bootstrap.getDriver(), bootstrap.getMyRepositoryId(), emptySet());
	}

	@Provides
	@Singleton
	OTDriver provideDriver(Eventloop eventloop, IAsyncHttpClient httpClient, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		String nodeUrl = config.get("node.address", DEMO_NODE_ADDRESS);
		GlobalOTNodeHttpClient service = GlobalOTNodeHttpClient.create(httpClient, nodeUrl);
		return new OTDriver(service, simKey);
	}

	@Provides
	@Singleton
	MyRepositoryId<EditorOperation> provideMyRepositoryId(Config config) {
		return config.get(ofMyRepositoryId(OPERATION_CODEC), "credentials", DEMO_MY_REPOSITORY_ID);
	}

}
