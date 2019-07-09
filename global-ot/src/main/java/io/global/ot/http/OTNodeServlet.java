package io.global.ot.http;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNode;
import io.datakernel.ot.OTNode.FetchData;
import io.datakernel.util.ParserFunction;
import io.global.ot.api.CommitId;
import io.global.ot.client.OTRepositoryAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

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
import static io.global.util.Utils.eitherComplete;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OTNodeServlet<K, D, C> implements AsyncServlet {
	private final RoutingServlet servlet;
	private final StructuredCodec<K> revisionCodec;
	private final StructuredCodec<FetchData<K, D>> fetchDataCodec;
	private final Function<C, byte[]> commitToBytes;
	private final ParserFunction<byte[], C> bytesToCommit;

	private Promise<@Nullable Void> closeNotification = new SettablePromise<>();

	private OTNodeServlet(OTNode<K, D, C> node, StructuredCodec<K> revisionCodec, StructuredCodec<D> diffCodec,
						  Function<C, byte[]> commitToBytes, ParserFunction<byte[], C> bytesToCommit) {
		servlet = getServlet(node);
		this.revisionCodec = revisionCodec;
		fetchDataCodec = FetchData.codec(revisionCodec, diffCodec);
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

	public void setCloseNotification(Promise<Void> closeNotification) {
		this.closeNotification = closeNotification;
	}

	private RoutingServlet getServlet(OTNode<K, D, C> node) {
		return RoutingServlet.create()
				.map(GET, "/" + CHECKOUT, request -> node.checkout()
						.map(checkoutData -> jsonResponse(fetchDataCodec, checkoutData)))
				.map(GET, "/" + FETCH, request -> {
					String id = request.getQueryParameter("id");
					if (id == null) {
						return Promise.ofException(HttpException.ofCode(400, "No 'id' query parameter"));
					}
					try {
						K currentCommitId = fromJson(revisionCodec, id);
						return node.fetch(currentCommitId)
								.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(GET, "/" + POLL, request -> {
					String id = request.getQueryParameter("id");
					if (id == null) {
						return Promise.ofException(HttpException.ofCode(400, "No 'id' query parameter"));
					}
					try {
						K currentCommitId = fromJson(revisionCodec, id);
						return eitherComplete(
								node.poll(currentCommitId)
										.map(fetchData -> jsonResponse(fetchDataCodec, fetchData)),
								closeNotification
										.map($2 -> HttpResponse.ofCode(503).withBody("Server closed".getBytes()))
						);
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map(POST, "/" + CREATE_COMMIT, loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								FetchData<K, D> fetchData = fromJson(fetchDataCodec, body.getString(UTF_8));
								return node.createCommit(fetchData.getCommitId(), fetchData.getDiffs(), fetchData.getLevel())
										.map(commit -> HttpResponse.ok200()
												.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)))
												.withBody(commitToBytes.apply(commit)));
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}))
				.map(POST, "/" + PUSH, loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								C commit = bytesToCommit.parse(body.getArray());
								return node.push(commit)
										.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}));
	}

	private static <T> HttpResponse jsonResponse(StructuredCodec<T> codec, T item) {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)))
				.withBody(toJson(codec, item).getBytes(UTF_8));
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return servlet.serve(request);
	}
}
