package io.global.ot.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNode;
import io.datakernel.ot.OTNode.FetchData;
import io.datakernel.util.ParserFunction;
import io.global.ot.api.CommitId;
import io.global.ot.client.OTRepositoryAdapter;

import java.util.function.Function;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.http.MediaTypes.PLAIN_TEXT;
import static io.datakernel.ot.OTNode.getFetchDataCodec;
import static io.global.ot.api.OTNodeCommand.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OTNodeServlet<K, D> implements WithMiddleware {
	private final MiddlewareServlet servlet;
	private final StructuredCodec<K> revisionCodec;
	private final StructuredCodec<FetchData<K, D>> fetchDataCodec;
	private final Function<OTCommit<K, D>, byte[]> commitToBytes;
	private final ParserFunction<byte[], OTCommit<K, D>> bytesToCommit;

	private OTNodeServlet(OTNode<K, D> node, StructuredCodec<K> revisionCodec, StructuredCodec<D> diffCodec,
			Function<OTCommit<K, D>, byte[]> commitToBytes, ParserFunction<byte[], OTCommit<K, D>> bytesToCommit) {
		this.servlet = getServlet(node);
		this.revisionCodec = revisionCodec;
		this.fetchDataCodec = getFetchDataCodec(revisionCodec, diffCodec);

		this.commitToBytes = commitToBytes;
		this.bytesToCommit = bytesToCommit;
	}

	public static <K, D> OTNodeServlet<K, D> create(OTNode<K, D> node, StructuredCodec<K> idCodec, StructuredCodec<D> diffCodec,
			Function<OTCommit<K, D>, byte[]> commitToBytes, ParserFunction<byte[], OTCommit<K, D>> bytesToCommit) {
		return new OTNodeServlet<>(node, idCodec, diffCodec, commitToBytes, bytesToCommit);
	}

	public static <D> OTNodeServlet<CommitId, D> forGlobalNode(OTNode<CommitId, D> node, StructuredCodec<D> diffCodec, OTRepositoryAdapter<D> adapter) {
		return new OTNodeServlet<>(node, REGISTRY.get(CommitId.class), diffCodec, adapter::commitToRawBytes, adapter::rawBytesToCommit);
	}

	@SuppressWarnings("unchecked")
	private MiddlewareServlet getServlet(OTNode<K, D> node) {
		return MiddlewareServlet.create()
				.with(GET, "/" + CHECKOUT, request -> node.checkout()
						.thenApply(checkoutData -> jsonResponse(fetchDataCodec, checkoutData)))
				.with(GET, "/" + FETCH, request -> {
					try {
						K revision = fromJson(revisionCodec, request.getQueryParameter("id"));
						return node.fetch(revision)
								.thenApply(fetchData -> jsonResponse(fetchDataCodec, fetchData));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + CREATE_COMMIT, request -> request.getBody()
						.thenCompose(body -> {
							try {
								FetchData<K, D> fetchData = fromJson(fetchDataCodec, body.getString(UTF_8));
								return node.createCommit(fetchData.getCommitId(), fetchData.getDiffs(), fetchData.getLevel())
										.thenApply(commit -> {
											OTCommit<K, D> otCommit = (OTCommit<K, D>) commit;
											byte[] data = commitToBytes.apply(otCommit);
											return HttpResponse.ok200()
													.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)))
													.withBody(data);
										});
							} catch (ParseException e) {
								return Promise.<HttpResponse>ofException(e);
							} finally {
								body.recycle();
							}
						}))
				.with(POST, "/" + PUSH, request -> request.getBody()
						.thenCompose(body -> {
							try {
								OTCommit<K, D> otCommit = bytesToCommit.parse(body.getArray());
								return node.push(otCommit)
										.thenApply(commitId -> jsonResponse(revisionCodec, commitId));
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

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
