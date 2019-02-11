package io.global.ot.graph;

import io.datakernel.async.Promise;
import io.datakernel.http.*;
import io.datakernel.ot.OTAlgorithms;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OTGraphServlet<K, D> implements AsyncServlet {
	@NotNull
	private OTAlgorithms<K, D> algorithms;
	private Function<K, String> idToString;
	private Function<D, String> diffToString;
	private Function<HttpRequest, Promise<K>> currentCommitFunction = $ -> null;

	private OTGraphServlet(@NotNull OTAlgorithms<K, D> algorithms, @Nullable Function<K, String> idToString, @Nullable Function<D, String> diffToString) {
		this.algorithms = algorithms;
		this.idToString = idToString;
		this.diffToString = diffToString;
	}

	public static <K, D> OTGraphServlet<K, D> create(@NotNull OTAlgorithms<K, D> algorithms, Function<K, String> idToString, Function<D, String> diffToString) {
		return new OTGraphServlet<>(algorithms, idToString, diffToString);
	}

	public OTGraphServlet<K, D> withCurrentCommit(@NotNull Function<HttpRequest, Promise<K>> revisionSupplier) {
		this.currentCommitFunction = revisionSupplier;
		return this;
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return currentCommitFunction.apply(request)
				.thenCompose(currentCommit -> algorithms.getRepository().getHeads()
						.thenCompose(heads -> algorithms.loadGraph(heads, idToString, diffToString))
						.thenApply(graph -> HttpResponse.ok200()
								.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT)))
								.withBody(graph.toGraphViz(currentCommit).getBytes(UTF_8))));
	}
}
