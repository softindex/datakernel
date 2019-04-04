package io.global.ot.graph;

import io.datakernel.async.Promise;
import io.datakernel.http.*;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTLoadedGraph;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.StringFormatUtils.limit;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OTGraphServlet<K, D> implements AsyncServlet {
	private static final Logger logger = LoggerFactory.getLogger(OTGraphServlet.class);

	public static final Function<CommitId, String> COMMIT_ID_TO_STRING = commitId -> limit(commitId.toString(), 7);

	@NotNull
	private final OTAlgorithms<K, D> algorithms;
	private final OTLoadedGraph<K, D> graph;
	private Function<HttpRequest, Promise<K>> currentCommitFunction = $ -> null;

	private OTGraphServlet(@NotNull OTAlgorithms<K, D> algorithms, @Nullable Function<K, String> idToString, @Nullable Function<D, String> diffToString) {
		this.algorithms = algorithms;
		graph = new OTLoadedGraph<>(algorithms.getOtSystem(), idToString, diffToString);
	}

	public static <K, D> OTGraphServlet<K, D> create(@NotNull OTAlgorithms<K, D> algorithms, Function<K, String> idToString, Function<D, String> diffToString) {
		return new OTGraphServlet<>(algorithms, idToString, diffToString);
	}

	public OTGraphServlet<K, D> withCurrentCommit(@NotNull Function<HttpRequest, Promise<K>> revisionSupplier) {
		currentCommitFunction = revisionSupplier;
		return this;
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return currentCommitFunction.apply(request)
				.then(currentCommit -> algorithms.getRepository().getHeads()
						.then(heads -> algorithms.loadGraph(heads, graph))
						.map(graph -> HttpResponse.ok200()
								.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT)))
								.withBody(graph.toGraphViz(currentCommit).getBytes(UTF_8))))
				.whenComplete(toLogger(logger, thisMethod(), request, this));
	}
}
