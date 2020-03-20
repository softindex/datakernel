package io.global.debug;

import com.google.gson.stream.JsonWriter;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Dependency;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Multibinder;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.util.Types;
import io.datakernel.http.*;
import io.datakernel.ot.OTLoadedGraph;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.api.GlobalKvNode;
import io.datakernel.kv.KvClient;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.UserContainer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.ot.OTAlgorithms.loadGraph;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.global.debug.DebugViewerModule.DebugView.OT;
import static io.global.ot.graph.OTGraphServlet.COMMIT_ID_TO_STRING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

public final class DebugViewerModule extends AbstractModule {
	private static final @NotNull HttpHeaderValue CONTENT_TYPE_GRAPHVIZ =
			ofContentType(ContentType.of(MediaType.of("text/vnd.graphviz"), UTF_8));

	private static final StructuredCodec<List<String>> STRING_LIST_CODEC = ofList(STRING_CODEC);
	private static final StructuredCodec<List<Tuple2<String, Long>>> FILE_LIST_CODEC = ofList(
			tuple(Tuple2::new,
					Tuple2::getValue1, STRING_CODEC,
					Tuple2::getValue2, LONG_CODEC));

	private static final StructuredCodec<List<Tuple3<String, String, Long>>> KV_ITEM_LIST_CODEC = ofList(
			tuple(Tuple3::new,
					Tuple3::getValue1, STRING_CODEC,
					Tuple3::getValue2, STRING_CODEC,
					Tuple3::getValue3, LONG_CODEC));

	private final EnumSet<DebugView> views;
	private final List<String> viewStrs;
	private final List<String> extraRepos;

	public DebugViewerModule(List<String> extraRepos, DebugView... views) {
		this.views = views.length == 0 && extraRepos.isEmpty() ?
				EnumSet.allOf(DebugView.class) :
				EnumSet.of(extraRepos.isEmpty() ? views[0] : OT, views);
		this.viewStrs = this.views.stream().map(v -> v.name().toLowerCase()).collect(toList());
		this.extraRepos = extraRepos;
	}

	public DebugViewerModule(DebugView... views) {
		this(emptyList(), views);
	}

	public enum DebugView {
		OT, FS, KV
	}

	@Override
	protected void configure() {
		if (views.contains(OT)) {
			scan(new OtViewer());
		}
		if (views.contains(DebugView.FS)) {
			scan(new FsViewer());
		}
		if (views.contains(DebugView.KV)) {
			scan(new KvViewer());
		}

		Dependency[] dependencies = new Dependency[views.size()];
		for (int i = 0; i < viewStrs.size(); i++) {
			dependencies[i] = Dependency.toKey(Key.of(AsyncServlet.class, viewStrs.get(i)));
		}

		bind(AsyncServlet.class)
				.export()
				.named("debug")
				.to(args -> {
					RoutingServlet router = RoutingServlet.create()
							.map(GET, "/api/check", request -> HttpResponse.ok200()
									.withJson(JsonUtils.toJson(STRING_LIST_CODEC, viewStrs)));
					for (int i = 0; i < viewStrs.size(); i++) {
						router.map("/api/" + viewStrs.get(i) + "/*", (AsyncServlet) args[i]);
					}
					return router;
				}, dependencies);

		bind(ObjectDisplayRegistry.class).in(ContainerScope.class).export();
		multibind(Key.of(ObjectDisplayRegistry.class), Multibinder.ofBinaryOperator(ObjectDisplayRegistry::merge));
	}

	private final class OtViewer {
		private Map<RepoID, SoftReference<OTLoadedGraph<CommitId, Object>>> loadedGraphs = new HashMap<>();

		@Provides
		@Named("ot")
		AsyncServlet otApi(Executor executor,
				GlobalOTNode otNode, OTDriver otDriver,
				TypedRepoNames repoNames, CodecFactory codecs,
				ContainerManager<?> containerManager
		) {
			String prefix = repoNames.getPrefix();
			return request -> {
				PubKey pk = request.getAttachment(UserContainer.class).getKeys().getPubKey();
				String path = request.getRelativePath();

				return otNode.list(pk)
						.then(list -> {
							if (path.isEmpty()) {
								return Promise.of(HttpResponse.ok200()
										.withJson(JsonUtils.toJson(STRING_LIST_CODEC, list.stream()
												.filter(x -> x.startsWith(prefix) || extraRepos.contains(x))
												.map(x -> x.startsWith(prefix) ? x.substring(prefix.length()) : x)
												.collect(toList()))));
							}
							String repo = extraRepos.contains(path) ? path : prefix + path;
							if (!list.contains(repo)) {
								return Promise.ofException(HttpException.ofCode(404, "No repo " + repo));
							}

							Key<?> diffType = repoNames.getKeyByRepo(repo);
							if (diffType == null) {
								return Promise.ofException(HttpException.ofCode(404, "No diff type for repo " + repo));
							}
							Injector containerInjector = containerManager.getContainerScope(pk);
							if (containerInjector == null) {
								return Promise.ofException(HttpException.ofCode(404, "No container for key " + pk));
							}
							OTSystem<Object> otSystem = containerInjector.getInstance(Key.ofType(Types.parameterized(OTSystem.class, diffType.getType())));
							ObjectDisplayRegistry prettyPrinter = containerInjector.getInstance(ObjectDisplayRegistry.class);

							StructuredCodec<Object> diffCodec = codecs.get(diffType.getType());
							RepoID repoID = RepoID.of(pk, repo);
							MyRepositoryId<Object> myRepositoryId = new MyRepositoryId<>(repoID, null, diffCodec);
							OTRepository<CommitId, Object> repository = new OTRepositoryAdapter<>(otDriver, myRepositoryId, emptySet());

							Function<Object, String> diffToString = object -> encode(diffType, prettyPrinter, object);

							SoftReference<OTLoadedGraph<CommitId, Object>> graphRef = loadedGraphs.computeIfAbsent(repoID, $ ->
									new SoftReference<>(new OTLoadedGraph<>(otSystem, COMMIT_ID_TO_STRING, diffToString)));

							OTLoadedGraph<CommitId, Object> graph = graphRef.get();
							if (graph == null) {
								graph = new OTLoadedGraph<>(otSystem, COMMIT_ID_TO_STRING, diffToString);
								loadedGraphs.put(repoID, new SoftReference<>(graph));
							}

							OTLoadedGraph<CommitId, Object> finalGraph = graph;
							return repository.getHeads()
									.then(heads -> loadGraph(repository, otSystem, heads, finalGraph))
									.map($ -> HttpResponse.ok200()
											.withHeader(CONTENT_TYPE, CONTENT_TYPE_GRAPHVIZ)
											.withBody(finalGraph.toGraphViz(null).getBytes(UTF_8)));
						});
			};
		}
	}

	private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

	private static String encode(Key<?> diffType, ObjectDisplayRegistry prettyPrinter, Object object) {
		try {
			StringWriter sw = new StringWriter();
			JsonWriter writer = new JsonWriter(sw);
			writer.beginArray();
			writer.value(prettyPrinter.getShortDisplay(diffType, object));
			writer.value(prettyPrinter.getLongDisplay(diffType, object));
			writer.endArray();
			return BASE64_ENCODER.encodeToString(sw.toString().getBytes(UTF_8));
		} catch (IOException e) {
			throw new AssertionError("I/O errors never happen when using StringWriter", e);
		}
	}

	private static final class FsViewer {

		@Provides
		@Named("fs")
		AsyncServlet fsViewer(GlobalFsNode fsNode, GlobalFsDriver driver, Executor executor, TypedRepoNames repoNames) {
			String repoNamesPrefix = repoNames.getPrefix();
			String prefix = repoNamesPrefix.equals("") ? repoNamesPrefix : ("ApplicationData/" + repoNamesPrefix);
			return request -> {
				String relativePath = request.getRelativePath();
				PubKey pk = request.getAttachment(UserContainer.class).getKeys().getPubKey();
				if (relativePath.isEmpty()) {
					return fsNode.listEntities(pk, "**")
							.map(list -> HttpResponse.ok200()
									.withJson(JsonUtils.toJson(FILE_LIST_CODEC, list.stream()
											.filter(signedCheckpoint -> signedCheckpoint.getValue().getFilename().startsWith(prefix))
											.map(signedCheckpoint -> {
												GlobalFsCheckpoint checkpoint = signedCheckpoint.getValue();
												return new Tuple2<>(checkpoint.getFilename().substring(prefix.length()), checkpoint.isTombstone() ? -1 : checkpoint.getPosition());
											})
											.collect(toList()))));
				}
				String path = UrlParser.urlDecode(relativePath);
				if (path == null) {
					return Promise.ofException(FILE_NOT_FOUND);
				}
				String filename = prefix + path;
				return driver.getMetadata(pk, filename)
						.then(meta -> {
							if (meta == null) {
								return Promise.ofException(HttpException.ofCode(404, "no such file"));
							}
							return HttpResponse.file(
									(offset, limit) -> driver.download(pk, filename, offset, limit),
									filename,
									meta.getPosition(),
									request.getHeader(HttpHeaders.RANGE),
									true
							);
						});
			};
		}
	}

	private static final class KvViewer {

		@SuppressWarnings("unchecked")
		@Provides
		@Named("kv")
		AsyncServlet kvApi(GlobalKvNode kvNode, Executor executor, TypedRepoNames repoNames, ContainerManager<?> containerManager) {
			String prefix = repoNames.getPrefix();
			return request -> {
				PubKey pk = request.getAttachment(UserContainer.class).getKeys().getPubKey();
				String path = request.getRelativePath();
				if (path.isEmpty()) {
					return kvNode.list(pk)
							.map(list -> HttpResponse.ok200()
									.withJson(JsonUtils.toJson(STRING_LIST_CODEC, list.stream()
											.filter(x -> x.startsWith(prefix))
											.map(x -> x.substring(prefix.length()))
											.collect(toList()))));
				}

				String table = prefix + path;
				Key<?> kvClientType = repoNames.getKeyByRepo(table);
				if (kvClientType == null) {
					return HttpException.ofCode(404, "No repo " + table);
				}
				Injector containerScope = containerManager.getContainerScope(pk);
				if (containerScope == null) {
					return HttpException.ofCode(404, "No container for key " + pk);
				}

				KvClient<Object, Object> kvClient = (KvClient<Object, Object>) containerScope.getInstance(kvClientType);

				return kvClient.download(table)
						.then(ChannelSupplier::toList)
						.map(list -> HttpResponse.ok200()
								.withJson(JsonUtils.toJson(KV_ITEM_LIST_CODEC, list.stream()
										.map(kvItem -> new Tuple3<>(kvItem.getKey().toString(), Objects.toString(kvItem.getValue()), kvItem.getTimestamp()))
										.collect(toList()))));
			};
		}
	}
}
