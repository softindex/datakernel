package io.global.debug;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.common.tuple.Tuple3;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.util.Types;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.ot.OTLoadedGraph;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.ot.OTAlgorithms.loadGraph;
import static io.global.ot.graph.OTGraphServlet.COMMIT_ID_TO_STRING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

public abstract class DebugViewerModule<C extends UserContainer> extends AbstractModule {
	private static final @NotNull HttpHeaderValue CONTENT_TYPE_GRAPHVIZ =
			HttpHeaderValue.ofContentType(ContentType.of(MediaType.of("text/vnd.graphviz"), UTF_8));

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

	private AsyncServlet createHtmlServingServlet(Executor executor, String type) {
		return StaticServlet.create(StaticLoader.ofClassPath(executor, ""), "debug.html")
				.withResponseBodyMapper(buf -> ByteBuf.wrapForReading(buf.asString(UTF_8).replace("{type}", type).getBytes(UTF_8)));
	}

	@Provides
	@Named("fs")
	AsyncServlet fsViewer(@Optional GlobalFsNode fsNode, GlobalFsDriver driver, Executor executor, TypedRepoNames repoNames) {
		if (fsNode == null || driver == null) {
			return request -> HttpException.ofCode(404, "No FS node used by this app");
		}
		return RoutingServlet.create()
				.map(GET, "/:pk", createHtmlServingServlet(executor, "fs"))
				.map(GET, "/:pk/*", request -> {
					PubKey pk;
					try {
						pk = PubKey.fromString(request.getPathParameter("pk"));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
					FsClient adapted = driver.adapt(pk);
					String filename = repoNames.getPrefix() + request.getRelativePath();
					return adapted.getMetadata(filename)
							.then(meta -> {
								if (meta == null) {
									return Promise.<HttpResponse>ofException(HttpException.ofCode(404, "no such file"));
								}
								return HttpResponse.file(
										(offset, limit) -> adapted.download(filename, offset, limit),
										filename,
										meta.getSize(),
										request.getHeader(HttpHeaders.RANGE),
										request.getQueryParameter("inline") != null
								);
							});
				})
				.map(GET, "/api/:pk", request -> {
					PubKey pk;
					try {
						pk = PubKey.fromString(request.getPathParameter("pk"));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
					String prefix = repoNames.getPrefix();
					return fsNode.listEntities(pk, "**")
							.map(list -> HttpResponse.ok200()
									.withJson(FILE_LIST_CODEC, list.stream()
											.filter(signedCheckpoint -> signedCheckpoint.getValue().getFilename().startsWith(prefix))
											.map(signedCheckpoint -> new Tuple2<>(signedCheckpoint.getValue().getFilename().substring(prefix.length()), signedCheckpoint.getValue().getPosition()))
											.collect(toList())));
				});
	}

	private static String diffToString(Object diff) {
		return diff.getClass().getSimpleName() + '|' + diff;
	}

	@Provides
	@Named("ot")
	AsyncServlet otViewer(Executor executor,
			@Optional GlobalOTNode otNode, @Optional OTDriver otDriver,
			TypedRepoNames repoNames, CodecFactory codecs, ContainerManager<C> containerManager
	) {
		if (otNode == null || otDriver == null) {
			return request -> HttpException.ofCode(404, "No FS node used by this app");
		}
		return RoutingServlet.create()
				.map(GET, "/:pk/*", createHtmlServingServlet(executor, "ot"))
				.map(GET, "/api/:pk/*", request -> {
					PubKey pk;
					try {
						pk = PubKey.fromString(request.getPathParameter("pk"));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
					String path = request.getRelativePath();
					String prefix = repoNames.getPrefix();
					if (path.isEmpty()) {
						return otNode.list(pk)
								.map(list -> HttpResponse.ok200()
										.withJson(STRING_LIST_CODEC, list.stream()
												.filter(x -> x.startsWith(prefix))
												.map(x -> x.substring(prefix.length()))
												.collect(toList())));
					}
					String repo = prefix + path;
					Key<?> diffType = repoNames.getKeyByRepo(repo);
					if (diffType == null) {
						return HttpException.ofCode(404, "No repo " + repo);
					}
					Injector containerScope = containerManager.getContainerScope(pk);
					if (containerScope == null) {
						return HttpException.ofCode(404, "No container for key " + pk);
					}

					OTSystem<Object> otSystem = containerScope.getInstance(Key.ofType(Types.parameterized(OTSystem.class, diffType.getType())));

					StructuredCodec<Object> diffCodec = codecs.get(diffType.getType());
					MyRepositoryId<Object> myRepositoryId = new MyRepositoryId<>(RepoID.of(pk, repo), null, diffCodec);

					OTRepository<CommitId, Object> repository = new OTRepositoryAdapter<>(otDriver, myRepositoryId, emptySet());

					return repository.getHeads()
							.then(heads -> loadGraph(repository, otSystem, heads, new OTLoadedGraph<>(otSystem, COMMIT_ID_TO_STRING, DebugViewerModule::diffToString)))
							.map(graph -> HttpResponse.ok200()
									.withHeader(CONTENT_TYPE, CONTENT_TYPE_GRAPHVIZ)
									.withBody(graph.toGraphViz().getBytes(UTF_8)));
				});
	}

	@SuppressWarnings("unchecked")
	@Provides
	@Named("kv")
	AsyncServlet kvViewer(@Optional GlobalKvNode kvNode, Executor executor, TypedRepoNames repoNames, ContainerManager<C> containerManager) {
		if (kvNode == null) {
			return request -> HttpException.ofCode(404, "No FS node used by this app");
		}
		return RoutingServlet.create()
				.map(GET, "/:pk/*", createHtmlServingServlet(executor, "kv"))
				.map(GET, "/api/:pk/*", request -> {
					PubKey pk;
					try {
						pk = PubKey.fromString(request.getPathParameter("pk"));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
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

	@Export
	@Provides
	@Named("debug")
	AsyncServlet debugServlet(@Named("fs") AsyncServlet fsServlet, @Named("ot") AsyncServlet otServlet, @Named("kv") AsyncServlet kvServlet, Executor executor) {
		return RoutingServlet.create()
				.map("/static/*", StaticServlet.create(StaticLoader.ofClassPath(executor, "debug-static")))
				.map("/fs/*", fsServlet)
				.map("/ot/*", otServlet)
				.map("/kv/*", kvServlet);
	}
}
