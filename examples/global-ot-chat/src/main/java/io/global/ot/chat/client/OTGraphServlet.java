package io.global.ot.chat.client;

import io.datakernel.async.Promise;
import io.datakernel.exception.ConstantException;
import io.datakernel.http.*;
import io.datakernel.ot.OTRepository;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OTGraphServlet<K, D> implements AsyncServlet {
	public static final ConstantException NO_REPOSITORY_SET = new ConstantException(OTGraphServlet.class, "Repository has not been set");

	private final Map<OTRepository<K, D>, NodesWalker<K, D>> walkersCache = new HashMap<>();
	@NotNull
	private final Function<K, String> idToString;
	@NotNull
	private final Function<D, String> diffToString;

	@Nullable
	private NodesWalker<K, D> currentWalker;

	private OTGraphServlet(@NotNull Function<K, String> idToString, @NotNull Function<D, String> diffToString) {
		this.idToString = idToString;
		this.diffToString = diffToString;
	}

	public static <K, D> OTGraphServlet<K, D> create(Function<K, String> idToString, Function<D, String> diffToString) {
		return new OTGraphServlet<>(idToString, diffToString);
	}

	public void changeRepository(OTRepository<K, D> repository) {
		currentWalker = walkersCache.computeIfAbsent(repository, repo -> NodesWalker.create(repo, idToString, diffToString));
	}

	@SuppressWarnings("unchecked")
	@Override
	public @NotNull Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		if (currentWalker == null) {
			return Promise.ofException(NO_REPOSITORY_SET);
		}

		NodesWalker<K, D>[] cachedWalker = new NodesWalker[]{currentWalker};
		return currentWalker.walk()
				.thenApply($ -> HttpResponse.ok200()
						.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT)))
						.withBody(cachedWalker[0].toGraphViz().getBytes(UTF_8)));
	}
}
