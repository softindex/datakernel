package io.global.ot.chat.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRepository;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.chat.common.Bootstrap;
import io.global.ot.chat.common.OTGraphServlet;
import io.global.ot.chat.operations.ChatOTState;
import io.global.ot.chat.operations.ChatOperation;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.http.GlobalOTNodeHttpClient;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.*;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.ot.GlobalOTConfigConverters.ofRepoID;
import static io.global.ot.chat.operations.Utils.*;
import static java.util.Collections.emptySet;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class ChatClientModule extends AbstractModule {
	private static final PrivKey DEMO_PRIVATE_KEY =
			PrivKey.of(new BigInteger("52a8fbf6c82e3e177a07d5fb822bbef07c1f28cfaeeb320964a4598ea82159b", 16));

	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	private static final RepoID DEMO_REPO_ID = RepoID.of(DEMO_PRIVATE_KEY.computePubKey(), "Example");
	private static final String DEMO_NODE_ADDRESS = "http://127.0.0.1:9000/ot/";
	private static final Path DEFAULT_RESOURCES_PATH = Paths.get("src/main/resources/static");
	private static final Duration DEFAULT_PULL_PUSH_INTERVAL = Duration.ofSeconds(5);

	@Override
	protected void configure() {
		bind(ChatOTState.class).in(Singleton.class);
	}

	@Provides
	@Singleton
	Eventloop provideEventloop(Config config) {
		return Eventloop.create()
				.initialize(ofEventloop(config));
	}

	@Provides
	@Singleton
	AsyncHttpServer provideServer(Eventloop eventloop, MiddlewareServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, ensureSessionID(servlet))
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	MiddlewareServlet provideMiddlewareServlet(ClientServlet apiServlet, StaticServlet staticServlet, SingleResourceStaticServlet chatHtmlServlet, OTGraphServlet<CommitId, ChatOperation> graphServlet) {
		return MiddlewareServlet.create()
				.with("/api", apiServlet)
				.with(GET, "/api/graph", graphServlet)
				.with(GET, "/chat/:user", chatHtmlServlet)
				.with(GET, "/", staticServlet)
				.withFallback(staticServlet);
	}

	@Provides
	@Singleton
	OTGraphServlet<CommitId, ChatOperation> provideGraphServlet(OTRepository<CommitId, ChatOperation> repository) {
		return OTGraphServlet.create(repository, getCommitIdToString(), getChatOperationToString());
	}

	@Provides
	@Singleton
	StaticServlet provideStaticServlet(Eventloop eventloop, Config config) {
		StaticLoader resourceLoader = StaticLoaders.ofPath(newCachedThreadPool(), config.get(ofPath(), "resources.path", DEFAULT_RESOURCES_PATH));
		return StaticServlet.create(eventloop, resourceLoader);
	}

	@Provides
	@Singleton
	SingleResourceStaticServlet provideCharHtmlServlet(Eventloop eventloop, Config config) {
		StaticLoader resourceLoader = StaticLoaders.ofPath(newCachedThreadPool(), config.get(ofPath(), "resources.path", DEFAULT_RESOURCES_PATH));
		return SingleResourceStaticServlet.create(eventloop, resourceLoader, "chat/chat.html");
	}

	@Provides
	@Singleton
	ClientServlet provideClientServlet(StateManagerProvider stateManagerProvider) {
		return ClientServlet.create(stateManagerProvider);
	}

	@Provides
	@Singleton
	StateManagerProvider provideManagerProvider(Eventloop eventloop, OTAlgorithms<CommitId, ChatOperation> algorithms, Config config) {
		return new StateManagerProvider(eventloop, algorithms,
				config.get(ofDuration(), "push.interval", DEFAULT_PULL_PUSH_INTERVAL),
				config.get(ofDuration(), "pull.interval", DEFAULT_PULL_PUSH_INTERVAL)
		);
	}

	@Provides
	@Singleton
	Bootstrap<ChatOperation> provideBootstrap(Eventloop eventloop, OTDriver driver, MyRepositoryId<ChatOperation> myRepositoryId) {
		return new Bootstrap<>(eventloop, driver, myRepositoryId);
	}

	@Provides
	@Singleton
	OTAlgorithms<CommitId, ChatOperation> provideAlgorithms(Eventloop eventloop, OTRepository<CommitId, ChatOperation> repository) {
		return OTAlgorithms.create(eventloop, createOTSystem(), repository);
	}

	@Provides
	@Singleton
	OTRepository<CommitId, ChatOperation> provideRepository(Eventloop eventloop, Bootstrap<ChatOperation> bootstrap) {
		return new OTRepositoryAdapter<>(bootstrap.getDriver(), bootstrap.getMyRepositoryId(), emptySet());
	}

	@Provides
	@Singleton
	OTDriver provideDriver(Eventloop eventloop, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		String nodeUrl = config.get("node.address", DEMO_NODE_ADDRESS);
		GlobalOTNodeHttpClient service = GlobalOTNodeHttpClient.create(AsyncHttpClient.create(eventloop), nodeUrl);
		return new OTDriver(service, simKey);
	}

	@Provides
	@Singleton
	MyRepositoryId<ChatOperation> provideMyRepositoryId(Config config) {
		RepoID repoID = config.get(ofRepoID(), "credentials.repositoryId", DEMO_REPO_ID);
		PrivKey privKey = config.get(ofPrivKey(), "credentials.privateKey", DEMO_PRIVATE_KEY);
		return new MyRepositoryId<>(repoID, privKey, ChatOperation.OPERATION_CODEC);
	}

	@Provides
	@Singleton
	@Named("Merge")
	EventloopTaskScheduler provideMergeScheduler(Eventloop eventloop, Config config, OTAlgorithms<CommitId, ChatOperation> algorithms) {
		return EventloopTaskScheduler.create(eventloop, algorithms::merge)
				.initialize(ofEventloopTaskScheduler(config.getChild("merge")));
	}
}
