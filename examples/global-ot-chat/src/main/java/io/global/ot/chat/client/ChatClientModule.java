package io.global.ot.chat.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.chat.operations.ChatOperation;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.graph.OTGraphServlet;
import io.global.ot.http.GlobalOTNodeHttpClient;
import io.global.ot.http.OTNodeServlet;
import io.global.ot.util.Bootstrap;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.ot.GlobalOTConfigConverters.ofMyRepositoryId;
import static io.global.ot.chat.operations.ChatOperation.OPERATION_CODEC;
import static io.global.ot.chat.operations.Utils.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.emptySet;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class ChatClientModule extends AbstractModule {
	private static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);
	private static final PrivKey DEMO_PRIVATE_KEY =
			PrivKey.of(new BigInteger("52a8fbf6c82e3e177a07d5fb822bbef07c1f28cfaeeb320964a4598ea82159b", 16));

	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	private static final RepoID DEMO_REPO_ID = RepoID.of(DEMO_PRIVATE_KEY.computePubKey(), "Example");
	private static final MyRepositoryId<ChatOperation> DEMO_MY_REPOSITORY_ID = new MyRepositoryId<>(DEMO_REPO_ID, DEMO_PRIVATE_KEY, OPERATION_CODEC);
	private static final String DEMO_NODE_ADDRESS = "http://127.0.0.1:9000/ot/";
	private static final Path DEFAULT_RESOURCES_PATH = Paths.get("front/build");

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
	MiddlewareServlet provideMiddlewareServlet(StaticServlet staticServlet, OTGraphServlet<CommitId, ChatOperation> graphServlet,
			OTNodeServlet<CommitId, ChatOperation, OTCommit<CommitId, ChatOperation>> nodeServlet) {
		return MiddlewareServlet.create()
				.with(GET, "/graph", graphServlet)
				.with("/node", nodeServlet)
				.withFallback(staticServlet);
	}

	@Provides
	@Singleton
	OTGraphServlet<CommitId, ChatOperation> provideGraphServlet(OTAlgorithms<CommitId, ChatOperation> algorithms) {
		return OTGraphServlet.create(algorithms, ID_TO_STRING, DIFF_TO_STRING)
				.withCurrentCommit(request -> {
					try {
						return Promise.of(fromJson(COMMIT_ID_CODEC, request.getQueryParameter("id")));
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
	OTNodeServlet<CommitId, ChatOperation, OTCommit<CommitId, ChatOperation>> provideNodeServlet(OTAlgorithms<CommitId, ChatOperation> algorithms, OTRepositoryAdapter<ChatOperation> repositoryAdapter) {
		return OTNodeServlet.forGlobalNode(algorithms.getOtNode(), OPERATION_CODEC, repositoryAdapter);
	}

	@Provides
	@Singleton
	Bootstrap<ChatOperation> provideBootstrap(Eventloop eventloop, OTDriver driver, MyRepositoryId<ChatOperation> myRepositoryId) {
		return new Bootstrap<>(eventloop, driver, myRepositoryId);
	}

	@Provides
	@Singleton
	OTAlgorithms<CommitId, ChatOperation> provideAlgorithms(Eventloop eventloop, OTRepositoryAdapter<ChatOperation> repository) {
		return OTAlgorithms.create(eventloop, createOTSystem(), repository);
	}

	@Provides
	@Singleton
	OTRepositoryAdapter<ChatOperation> provideRepository(Eventloop eventloop, Bootstrap<ChatOperation> bootstrap) {
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
		return config.get(ofMyRepositoryId(OPERATION_CODEC), "credentials", DEMO_MY_REPOSITORY_ID);
	}

}
