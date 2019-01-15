package io.global.ot.chat.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;
import io.global.ot.chat.common.Gateway;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.ot.chat.common.GatewayCommand.*;
import static io.global.ot.chat.common.Utils.URL_ENCODED_COMMIT_ID;
import static io.global.ot.chat.common.Utils.getTupleCodec;
import static io.global.ot.util.HttpDataFormats.urlDecodeCommitId;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GatewayServlet<D> implements WithMiddleware {
	private final MiddlewareServlet servlet;
	private final StructuredCodec<D> diffCodec;
	private final StructuredCodec<Tuple2<CommitId, List<D>>> tupleCodec;

	private GatewayServlet(Gateway<D> clientServerApi, StructuredCodec<D> diffCodec) {
		this.servlet = servlet(clientServerApi);
		this.diffCodec = diffCodec;
		this.tupleCodec = getTupleCodec(diffCodec);
	}

	public static <D> GatewayServlet<D> create(Gateway<D> gateway, StructuredCodec<D> diffCodec) {
		return new GatewayServlet<>(gateway, diffCodec);
	}

	private MiddlewareServlet servlet(Gateway<D> gateway) {
		return MiddlewareServlet.create()
				.with(GET, "/" + CHECKOUT, request -> gateway.checkout()
						.thenApply(tuple -> jsonResponse(tuple, tupleCodec)))
				.with(GET, "/" + PULL + "/:commitId", request -> {
					try {
						return gateway.pull(urlDecodeCommitId(request.getPathParameter("commitId")))
								.thenApply(tuple -> jsonResponse(tuple, tupleCodec));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + PUSH + "/:commitId", request -> request.getBody()
						.thenCompose(body -> {
							try {
								CommitId commitID = urlDecodeCommitId(request.getPathParameter("commitId"));
								List<D> diffs = fromJson(ofList(diffCodec), body.getString(UTF_8));
								return gateway.push(commitID, diffs);
							} catch (ParseException e) {
								return Promise.ofException(e);
							} finally {
								body.recycle();
							}
						})
						.thenApply(commitId -> jsonResponse(commitId, URL_ENCODED_COMMIT_ID)));
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}

	private static <T> HttpResponse jsonResponse(T value, StructuredCodec<T> codec) {
		return HttpResponse.ok200()
				.withBody(toJson(codec, value).getBytes(UTF_8))
				.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON)));
	}
}
