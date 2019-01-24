package io.global.ot.chat.common;

import io.datakernel.async.Promise;
import io.datakernel.http.*;
import io.datakernel.ot.OTRepository;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OTGraphServlet<K, D> implements AsyncServlet {
	@NotNull
	private NodesWalker<K, D> walker;

	private OTGraphServlet(@NotNull NodesWalker<K, D> walker) {
		this.walker = walker;
	}

	public static <K, D> OTGraphServlet<K, D> create(@NotNull OTRepository<K, D> repository, Function<K, String> idToString, Function<D, String> diffToString) {
		NodesWalker<K, D> walker = NodesWalker.create(repository, idToString, diffToString);
		return new OTGraphServlet<>(walker);
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return walker.walk()
				.thenApply($ -> HttpResponse.ok200()
						.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT)))
						.withBody(walker.toGraphViz().getBytes(UTF_8)));
	}
}
