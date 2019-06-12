package io.global.ot;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.*;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNode.FetchData;
import io.datakernel.ot.OTNodeImpl;
import io.datakernel.ot.OTSystem;
import io.global.common.PrivKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import org.jetbrains.annotations.NotNull;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.http.MediaTypes.PLAIN_TEXT;
import static io.global.ot.api.OTNodeCommand.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;

public final class DynamicOTNodeServlet<D> implements AsyncServlet {
	public static final ParseException ID_REQUIRED = new ParseException(DynamicOTNodeServlet.class, "Query parameter ID is required");
	public static final ParseException KEY_REQUIRED = new ParseException(DynamicOTNodeServlet.class, "Cookie 'Key' is required");
	private static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);

	private final AsyncServlet servlet;
	private final OTSystem<D> otSystem;
	private final OTDriver driver;
	private final StructuredCodec<D> diffCodec;
	private final StructuredCodec<FetchData<CommitId, D>> fetchDataCodec;
	private final String prefix;

	private DynamicOTNodeServlet(OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> diffCodec, String prefix) {
		this.servlet = getServlet();
		this.diffCodec = diffCodec;
		this.fetchDataCodec = FetchData.codec(COMMIT_ID_CODEC, diffCodec);
		this.otSystem = otSystem;
		this.driver = driver;
		this.prefix = prefix;
	}

	public static <D> DynamicOTNodeServlet<D> create(OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> diffCodec, String prefix) {
		return new DynamicOTNodeServlet<>(driver, otSystem, diffCodec, prefix);
	}

	private RoutingServlet getServlet() {
		return RoutingServlet.create()
				.with(GET, "/" + CHECKOUT, request -> {
					try {
						return getNode(request).checkout()
								.map(checkoutData -> jsonResponse(fetchDataCodec, checkoutData));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + FETCH, request -> {
					try {
						String id = request.getQueryParameter("id");
						if (id == null) {
							throw ID_REQUIRED;
						}
						CommitId currentCommitId = fromJson(COMMIT_ID_CODEC, id);
						return getNode(request).fetch(currentCommitId)
								.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + POLL, request -> {
					try {
						String id = request.getQueryParameter("id");
						if (id == null) {
							throw ID_REQUIRED;
						}
						CommitId currentCommitId = fromJson(COMMIT_ID_CODEC, id);
						return getNode(request).poll(currentCommitId)
								.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + CREATE_COMMIT, loadBody().serve(request -> {
					try {
						FetchData<CommitId, D> fetchData = fromJson(fetchDataCodec, request.getBody().getString(UTF_8));
						return getNode(request).createCommit(fetchData.getCommitId(), fetchData.getDiffs(), fetchData.getLevel())
								.map(commit -> {
									assert commit.getSerializedData() != null;
									return HttpResponse.ok200()
											.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)))
											.withBody(commit.getSerializedData());
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				}))
				.with(POST, "/" + PUSH, loadBody().serve(request -> {
							try {
								OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> node = getNode(request);
								OTCommit<CommitId, D> commit = ((OTRepositoryAdapter<D>) node.getRepository()).parseRawBytes(request.getBody().getArray());
								return node.push(commit)
										.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}));
	}

	private static <T> HttpResponse jsonResponse(StructuredCodec<T> codec, T item) {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)))
				.withBody(toJson(codec, item).getBytes(UTF_8));
	}

	private OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> getNode(HttpRequest request) throws ParseException {
		String key = request.getCookie("Key");
		if (key == null) {
			throw KEY_REQUIRED;
		}
		PrivKey privKey = PrivKey.fromString(key);
		String suffix = request.getPathParameter("suffix");
		String repositoryName = prefix + (suffix == null ? "" : ('/' + suffix));
		RepoID repoID = RepoID.of(privKey, repositoryName);
		MyRepositoryId<D> myRepositoryId = new MyRepositoryId<>(repoID, privKey, diffCodec);
		OTRepositoryAdapter<D> adapter = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		return OTNodeImpl.create(adapter, otSystem);
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		return servlet.serve(request);
	}
}
