package io.global.common.ot;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.ot.*;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.graph.OTGraphServlet;
import io.global.ot.http.OTNodeServlet;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.datakernel.loader.StaticLoader.ofClassPath;
import static io.global.common.ExampleCommonModule.DEMO_PRIVATE_KEY;
import static io.global.common.ExampleCommonModule.DEMO_SIM_KEY;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.ot.GlobalOTConfigConverters.ofMyRepositoryId;
import static io.global.ot.graph.OTGraphServlet.COMMIT_ID_TO_STRING;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.emptySet;

public class OTCommonModule<D> extends AbstractModule {
	public static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);
	public static final RepoID DEMO_REPO_ID = RepoID.of(DEMO_PRIVATE_KEY.computePubKey(), "Chat Example");
	public static final Duration DEFAULT_PUSH_DELAY_DURATION = Duration.ofSeconds(1);

	@Provides
	@Named("Example")
	AsyncHttpServer server(Eventloop eventloop, RoutingServlet servlet,
			OTNodeServlet<CommitId, D, OTCommit<CommitId, D>> nodeServlet, Config config) {

		AsyncHttpServer asyncHttpServer = AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));

		nodeServlet.setCloseNotification(asyncHttpServer.getCloseNotification());

		return asyncHttpServer;
	}

	@Provides
	RoutingServlet middlewareServlet(StaticServlet staticServlet, OTGraphServlet<CommitId, D> graphServlet,
			OTNodeServlet<CommitId, D, OTCommit<CommitId, D>> nodeServlet) {
		return RoutingServlet.create()
				.with(GET, "/graph/*", graphServlet)
				.with("/node/*", nodeServlet)
				.with("/*", staticServlet);
	}

	@Provides
	OTGraphServlet<CommitId, D> graphServlet(OTRepository<CommitId, D> repository, OTSystem<D> otSystem,
			Function<D, String> diffToString) {
		return OTGraphServlet.create(repository, otSystem, COMMIT_ID_TO_STRING, diffToString)
				.withCurrentCommit(request -> {
					String id = request.getQueryParameter("id");
					if (id == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						return Promise.of(fromJson(COMMIT_ID_CODEC, id));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Provides
	StaticServlet staticServlet(Config config) {
		return StaticServlet.create(ofClassPath(config.get(ofString(), "resources.path")))
				.withIndexHtml();
	}

	@Provides
	OTNodeServlet<CommitId, D, OTCommit<CommitId, D>> nodeServlet(OTNode<CommitId, D, OTCommit<CommitId, D>> node,
			StructuredCodec<D> diffCodec, OTRepository<CommitId, D> repository, Config config) {
		Duration delay = config.get(ofDuration(), "push.delay", DEFAULT_PUSH_DELAY_DURATION);
		return OTNodeServlet.forGlobalNode(DelayedPushNode.create(node, delay), diffCodec, (OTRepositoryAdapter<D>) repository);
	}

	@Provides
	OTNode<CommitId, D, OTCommit<CommitId, D>> node(OTRepository<CommitId, D> repository, OTSystem<D> otSystem) {
		return OTNodeImpl.create(repository, otSystem);
	}

	@Provides
	OTRepository<CommitId, D> repository(OTDriver driver, MyRepositoryId<D> myRepositoryId) {
		return new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
	}

	@Provides
	OTDriver driver(GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	MyRepositoryId<D> myRepositoryId(Config config, StructuredCodec<D> diffCodec) {
		MyRepositoryId<D> DEMO_MY_REPOSITORY_ID = new MyRepositoryId<>(DEMO_REPO_ID, DEMO_PRIVATE_KEY, diffCodec);
		return config.get(ofMyRepositoryId(diffCodec), "credentials", DEMO_MY_REPOSITORY_ID);
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config.getChild("executor"));
	}
}
