package io.global.ot;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.*;
import io.datakernel.ot.OTLoadedGraph;
import io.datakernel.ot.OTRepository;
import io.datakernel.ot.OTSystem;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.ot.OTAlgorithms.loadGraph;
import static io.datakernel.util.StringFormatUtils.limit;
import static io.global.ot.DynamicOTNodeServlet.KEYS_REQUIRED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;

public final class DynamicOTGraphServlet<D> implements AsyncServlet {
	private static final Function<CommitId, String> COMMIT_ID_TO_STRING = commitId -> limit(commitId.toString(), 7);

	private final OTDriver driver;
	private final OTSystem<D> otSystem;
	private final StructuredCodec<D> diffCodec;
	private final String repoPrefix;
	private final Function<D, String> diffToString;

	private DynamicOTGraphServlet(OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> diffCodec,
			String repoPrefix, Function<D, String> diffToString) {
		this.driver = driver;
		this.otSystem = otSystem;
		this.diffCodec = diffCodec;
		this.repoPrefix = repoPrefix;
		this.diffToString = diffToString;
	}

	public static <D> DynamicOTGraphServlet<D> create(OTDriver driver, OTSystem<D> otSystem,
			StructuredCodec<D> diffCodec, String repoPrefix, Function<D, String> diffToString) {
		return new DynamicOTGraphServlet<>(driver, otSystem, diffCodec, repoPrefix, diffToString);
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		try {
			String suffix = request.getPathParameters().get("suffix");
			String repositoryName = repoPrefix + (suffix == null ? "" : ('/' + suffix));
			String key = request.getCookie("Key");
			if (key == null) return Promise.ofException(HttpException.ofCode(400, "Cookie 'Key' is required"));
			KeyPair keys = PrivKey.fromString(key).computeKeys();
			RepoID repoID = RepoID.of(keys, repositoryName);
			MyRepositoryId<D> myRepositoryId = new MyRepositoryId<>(repoID, keys.getPrivKey(), diffCodec);
			OTRepository<CommitId, D> repository = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
			return repository.getHeads()
					.then(heads -> loadGraph(repository, otSystem, heads, new OTLoadedGraph<>(otSystem, COMMIT_ID_TO_STRING, diffToString)))
					.map(graph -> HttpResponse.ok200()
							.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.PLAIN_TEXT)))
							.withBody(graph.toGraphViz().getBytes(UTF_8)));
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}

	private OTRepository<CommitId, D> getRepo(HttpRequest request) throws ParseException {
		String suffix = request.getPathParameters().get("suffix");
		String repositoryName = repoPrefix + (suffix == null ? "" : ('/' + suffix));
		String key = request.getCookie("Key");
		if (key == null) throw KEYS_REQUIRED;
		KeyPair keys = PrivKey.fromString(key).computeKeys();
		RepoID repoID = RepoID.of(keys, repositoryName);
		MyRepositoryId<D> myRepositoryId = new MyRepositoryId<>(repoID, keys.getPrivKey(), diffCodec);
		return new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
	}
}