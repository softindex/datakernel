package io.global.chat;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
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

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
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

public final class DynamicOTNodeServlet<D> implements WithMiddleware {
	private static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);

	private final MiddlewareServlet servlet;
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

	private MiddlewareServlet getServlet() {
		return MiddlewareServlet.create()
				.with(GET, "/" + CHECKOUT, request -> getNode(request).checkout()
						.map(checkoutData -> jsonResponse(fetchDataCodec, checkoutData)))
				.with(GET, "/" + FETCH, request -> {
					try {
						CommitId currentCommitId = fromJson(COMMIT_ID_CODEC, request.getQueryParameter("id"));
						return getNode(request).fetch(currentCommitId)
								.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + POLL, request -> {
					try {
						CommitId currentCommitId = fromJson(COMMIT_ID_CODEC, request.getQueryParameter("id"));
						return getNode(request).poll(currentCommitId)
								.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + CREATE_COMMIT, request -> request.getBody()
						.then(body -> {
							try {
								FetchData<CommitId, D> fetchData = fromJson(fetchDataCodec, body.getString(UTF_8));
								return getNode(request).createCommit(fetchData.getCommitId(), fetchData.getDiffs(), fetchData.getLevel())
										.map(commit -> {
											assert commit.getSerializedData() != null;
											return HttpResponse.ok200()
													.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)))
													.withBody(commit.getSerializedData());
										});
							} catch (ParseException e) {
								return Promise.<HttpResponse>ofException(e);
							} finally {
								body.recycle();
							}
						}))
				.with(POST, "/" + PUSH, request -> request.getBody()
						.then(body -> {
							try {
								OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> node = getNode(request);
								OTCommit<CommitId, D> commit = ((OTRepositoryAdapter<D>) node.getRepository()).parseRawBytes(body.getArray());
								return node.push(commit)
										.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
							} catch (ParseException e) {
								return Promise.<HttpResponse>ofException(e);
							} finally {
								body.recycle();
							}
						}));
	}

	private static <T> HttpResponse jsonResponse(StructuredCodec<T> codec, T item) {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)))
				.withBody(toJson(codec, item).getBytes(UTF_8));
	}

	private OTNodeImpl<CommitId, D, OTCommit<CommitId, D>> getNode(HttpRequest request) {
		PrivKey privKey = request.get(PrivKey.class);
		String suffix = request.getPathParameterOrNull("suffix");
		String repositoryName = prefix + (suffix == null ? "" : ('/' + suffix));
		RepoID repoID = RepoID.of(privKey, repositoryName);
		MyRepositoryId<D> myRepositoryId = new MyRepositoryId<>(repoID, privKey, diffCodec);
		OTRepositoryAdapter<D> adapter = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		return OTNodeImpl.create(adapter, otSystem);
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
