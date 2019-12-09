package io.global.debug;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Dependency;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Multibinder;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.util.Types;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.ot.OTLoadedGraph;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvClient;
import io.global.ot.TypedRepoNames;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.UserContainer;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.ContentTypes.HTML_UTF_8;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.ot.OTAlgorithms.loadGraph;
import static io.global.ot.graph.OTGraphServlet.COMMIT_ID_TO_STRING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

public abstract class DebugViewerModule<C extends UserContainer> extends AbstractModule {
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

	public DebugViewerModule(DebugView... views) {
		this.views = views.length == 0 ?
				EnumSet.allOf(DebugView.class) :
				EnumSet.of(views[0], views);
		this.viewStrs = this.views.stream().map(v -> v.name().toLowerCase()).collect(toList());
	}

	public enum DebugView {
		OT, FS, KV
	}

	private AsyncServlet createHtmlServingServlet(Executor executor, String type, Type containerType) {
		StaticLoader loader = StaticLoader.ofClassPath(executor, "");
		return request ->
				loader.load("debug.html")
						.map(buf -> HttpResponse.ok200()
								.withHeader(CONTENT_TYPE, ofContentType(HTML_UTF_8))
								.withBody(ByteBuf.wrapForReading(buf.asString(UTF_8)
										.replace("{pk}", ((UserContainer) request.getAttachment(containerType)).getKeys().getPubKey().asString())
										.replace("{type}", type)
										.replace("{enabled_types}", "['" + String.join("', '", viewStrs) + "']")
										.getBytes(UTF_8))));
	}

	@Override
	protected void configure() {
		if (views.contains(DebugView.OT)) {
			scan(new OtViewer());
		}
		if (views.contains(DebugView.FS)) {
			scan(new FsViewer());
		}
		if (views.contains(DebugView.KV)) {
			scan(new KvViewer());
		}

		Dependency[] dependencies = new Dependency[1 + views.size()];
		dependencies[0] = Dependency.toKey(Key.of(Executor.class));
		for (int i = 0; i < views.size(); i++) {
			dependencies[i + 1] = Dependency.toKey(Key.of(AsyncServlet.class, viewStrs.get(i)));
		}

		bind(AsyncServlet.class)
				.export()
				.named("debug")
				.to(args -> {
					Executor executor = (Executor) args[0];
					RoutingServlet router = RoutingServlet.create()
							.map("/static/*", StaticServlet.create(StaticLoader.ofClassPath(executor, "debug-static")));
					for (int i = 0; i < viewStrs.size(); i++) {
						router.map("/" + viewStrs.get(i) + "/*", (AsyncServlet) args[i + 1]);
					}
					return router;
				}, dependencies);

		multibind(Key.of(ObjectDisplayRegistry.class), Multibinder.ofBinaryOperator(ObjectDisplayRegistry::merge));
	}

	private final class OtViewer {
		private Map<RepoID, SoftReference<OTLoadedGraph<CommitId, Object>>> loadedGraphs = new HashMap<>();

		@Provides
		@Named("ot")
		AsyncServlet otViewer(Executor executor,
				@Optional GlobalOTNode otNode, @Optional OTDriver otDriver,
				TypedRepoNames repoNames, CodecFactory codecs,
				ContainerManager<C> containerManager, Key<C> reifiedC
		) {
			if (otNode == null || otDriver == null) {
				return request -> HttpException.ofCode(404, "No FS node used by this app");
			}
			return RoutingServlet.create()
					.map(GET, "/*", createHtmlServingServlet(executor, "ot", reifiedC.getType()))
					.map(GET, "/api/*", request -> {
						PubKey pk = ((UserContainer) request.getAttachment(reifiedC.getType())).getKeys().getPubKey();
						String path = request.getRelativePath();
						String prefix = repoNames.getPrefix();

						return otNode.list(pk)
								.then(list -> {
									if (path.isEmpty()) {
										return Promise.of(HttpResponse.ok200()
												.withJson(STRING_LIST_CODEC, list.stream()
														.filter(x -> x.startsWith(prefix))
														.map(x -> x.substring(prefix.length()))
														.collect(toList())));
									}
									String repo = prefix + path;
									if (!list.contains(repo)) {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "No repo " + repo));
									}

									Key<?> diffType = repoNames.getKeyByRepo(repo);
									if (diffType == null) {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "No diff type for repo " + repo));
									}
									Injector containerInjector = containerManager.getContainerScope(pk);
									if (containerInjector == null) {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "No container for key " + pk));
									}
									OTSystem<Object> otSystem = containerInjector.getInstance(Key.ofType(Types.parameterized(OTSystem.class, diffType.getType())));
									ObjectDisplayRegistry prettyPrinter = containerInjector.getInstance(ObjectDisplayRegistry.class);

									StructuredCodec<Object> diffCodec = codecs.get(diffType.getType());
									RepoID repoID = RepoID.of(pk, repo);
									MyRepositoryId<Object> myRepositoryId = new MyRepositoryId<>(repoID, null, diffCodec);
									OTRepository<CommitId, Object> repository = new OTRepositoryAdapter<>(otDriver, myRepositoryId, emptySet());

									Function<Object, String> diffToString = object ->
											prettyPrinter.getShortDisplay(object).replaceAll("\"", "\\\\\\\"") + "|" + prettyPrinter.getLongDisplay(object).replaceAll("\"", "\\\\\\\"");

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
					});
		}
	}

	private final class FsViewer {

		@Provides
		@Named("fs")
		AsyncServlet fsViewer(@Optional GlobalFsNode fsNode, GlobalFsDriver driver, Executor executor, TypedRepoNames repoNames, Key<C> reifiedC) {
			if (fsNode == null || driver == null) {
				return request -> HttpException.ofCode(404, "No FS node used by this app");
			}
			return RoutingServlet.create()
					.map(GET, "/", createHtmlServingServlet(executor, "fs", reifiedC.getType()))
					.map(GET, "/*", request -> {
						PubKey pk = ((UserContainer) request.getAttachment(reifiedC.getType())).getKeys().getPubKey();
						String filename = repoNames.getPrefix() + request.getRelativePath();
						return driver.getMetadata(pk, filename)
								.then(meta -> {
									if (meta == null) {
										return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "no such file"));
									}
									return HttpResponse.file(
											(offset, limit) -> driver.download(pk, filename, offset, limit),
											filename,
											meta.getPosition(),
											request.getHeader(HttpHeaders.RANGE),
											true
									);
								});
					})
					.map(GET, "/api", request -> {
						PubKey pk = ((UserContainer) request.getAttachment(reifiedC.getType())).getKeys().getPubKey();
						String prefix = repoNames.getPrefix();
						return fsNode.listEntities(pk, "**")
								.map(list -> HttpResponse.ok200()
										.withJson(FILE_LIST_CODEC, list.stream()
												.filter(signedCheckpoint -> signedCheckpoint.getValue().getFilename().startsWith(prefix))
												.map(signedCheckpoint -> new Tuple2<>(signedCheckpoint.getValue().getFilename().substring(prefix.length()), signedCheckpoint.getValue().getPosition()))
												.collect(toList())));
					});
		}
	}

	private final class KvViewer {

		@SuppressWarnings("unchecked")
		@Provides
		@Named("kv")
		AsyncServlet kvViewer(@Optional GlobalKvNode kvNode, Executor executor, TypedRepoNames repoNames, ContainerManager<C> containerManager, Key<C> reifiedC) {
			if (kvNode == null) {
				return request -> HttpException.ofCode(404, "No FS node used by this app");
			}
			return RoutingServlet.create()
					.map(GET, "/*", createHtmlServingServlet(executor, "kv", reifiedC.getType()))
					.map(GET, "/api/*", request -> {
						PubKey pk = ((UserContainer) request.getAttachment(reifiedC.getType())).getKeys().getPubKey();
						String path = request.getRelativePath();
						String prefix = repoNames.getPrefix();
						if (path.isEmpty()) {
							return kvNode.list(pk)
									.map(list -> HttpResponse.ok200()
											.withJson(STRING_LIST_CODEC, list.stream()
													.filter(x -> x.startsWith(prefix))
													.map(x -> x.substring(prefix.length()))
													.collect(toList())));
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
										.withJson(KV_ITEM_LIST_CODEC, list.stream()
												.map(kvItem -> new Tuple3<>(kvItem.getKey().toString(), Objects.toString(kvItem.getValue()), kvItem.getTimestamp()))
												.collect(toList())));
					});
		}
	}
}
