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
import static io.global.ot.api.OTNodeCommand.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OTNodeServlet<K, D, C> implements WithMiddleware {
	private final MiddlewareServlet servlet;
	private final StructuredCodec<K> revisionCodec;
	private final StructuredCodec<FetchData<K, D>> fetchDataCodec;
	private final Function<C, byte[]> commitToBytes;
	private final ParserFunction<byte[], C> bytesToCommit;

	private OTNodeServlet(OTNode<K, D, C> node, StructuredCodec<K> revisionCodec, StructuredCodec<D> diffCodec,
			Function<C, byte[]> commitToBytes, ParserFunction<byte[], C> bytesToCommit) {
		this.servlet = getServlet(node);
		this.revisionCodec = revisionCodec;
		this.fetchDataCodec = FetchData.codec(revisionCodec, diffCodec);
		this.commitToBytes = commitToBytes;
		this.bytesToCommit = bytesToCommit;
	}

	public static <K, D, C> OTNodeServlet<K, D, C> create(OTNode<K, D, C> node, StructuredCodec<K> idCodec, StructuredCodec<D> diffCodec,
			Function<C, byte[]> commitToBytes, ParserFunction<byte[], C> bytesToCommit) {
		return new OTNodeServlet<>(node, idCodec, diffCodec, commitToBytes, bytesToCommit);
	}

	public static <D> OTNodeServlet<CommitId, D, OTCommit<CommitId, D>> forGlobalNode(OTNode<CommitId, D, OTCommit<CommitId, D>> node,
			StructuredCodec<D> diffCodec, OTRepositoryAdapter<D> adapter) {
		return new OTNodeServlet<>(node, REGISTRY.get(CommitId.class), diffCodec, OTCommit::getSerializedData, adapter::parseRawBytes);
	}

	private MiddlewareServlet getServlet(OTNode<K, D, C> node) {
		return MiddlewareServlet.create()
				.with(GET, "/" + CHECKOUT, request -> node.checkout()
						.thenApply(checkoutData -> jsonResponse(fetchDataCodec, checkoutData)))
				.with(GET, "/" + FETCH, request -> {
					try {
						K currentCommitId = fromJson(revisionCodec, request.getQueryParameter("id"));
						return node.fetch(currentCommitId)
								.thenApply(fetchData -> jsonResponse(fetchDataCodec, fetchData));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + POLL, request -> {
					try {
						K currentCommitId = fromJson(revisionCodec, request.getQueryParameter("id"));
						return node.poll(currentCommitId)
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
										.thenApply(commit -> HttpResponse.ok200()
												.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)))
												.withBody(commitToBytes.apply(commit)));
							} catch (ParseException e) {
								return Promise.<HttpResponse>ofException(e);
							} finally {
								body.recycle();
							}
						}))
				.with(POST, "/" + PUSH, request -> request.getBody()
						.thenCompose(body -> {
							try {
								C commit = bytesToCommit.parse(body.getArray());
								return node.push(commit)
										.thenApply(fetchData -> jsonResponse(fetchDataCodec, fetchData));
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
