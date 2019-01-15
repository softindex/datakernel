package io.global.ot.chat.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;
import io.global.ot.chat.common.Gateway;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.global.ot.chat.common.GatewayCommand.*;
import static io.global.ot.chat.common.Utils.URL_ENCODED_COMMIT_ID;
import static io.global.ot.chat.common.Utils.getTupleCodec;
import static io.global.ot.util.HttpDataFormats.urlEncodeCommitId;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GatewayHttpClient<D> implements Gateway<D> {
	private final IAsyncHttpClient httpClient;
	private final String url;
	private final StructuredCodec<D> diffCodec;
	private final StructuredCodec<Tuple2<CommitId, List<D>>> tupleCodec;

	private GatewayHttpClient(IAsyncHttpClient httpClient, String url, StructuredCodec<D> diffCodec) {
		this.httpClient = httpClient;
		this.url = url.endsWith("/") ? url : url + '/';
		this.diffCodec = diffCodec;
		this.tupleCodec = getTupleCodec(diffCodec);
	}

	public static <D> GatewayHttpClient<D> create(IAsyncHttpClient httpClient, String url, StructuredCodec<D> diffCodec) {
		return new GatewayHttpClient<>(httpClient, url, diffCodec);
	}

	@Override
	public Promise<Tuple2<CommitId, List<D>>> checkout() {
		return httpClient.request(HttpRequest.get(url + CHECKOUT))
				.thenCompose(res -> processResult(res, tupleCodec));
	}

	@Override
	public Promise<Tuple2<CommitId, List<D>>> pull(CommitId oldId) {
		return httpClient.request(HttpRequest.get(url + PULL + "/" + urlEncodeCommitId(oldId)))
				.thenCompose(res -> processResult(res, tupleCodec));
	}

	@Override
	public Promise<CommitId> push(CommitId currentId, List<D> clientDiffs) {
		return httpClient.request(HttpRequest.post(url + PUSH + "/" + urlEncodeCommitId(currentId))
				.withBody(toJson(ofList(diffCodec), clientDiffs).getBytes(UTF_8)))
				.thenCompose(res -> processResult(res, URL_ENCODED_COMMIT_ID));
	}

	private static <T> Promise<T> processResult(HttpResponse res, StructuredCodec<T> json) {
		return res.getBody()
				.thenCompose(body -> {
					try {
						if (res.getCode() != 200) return Promise.ofException(HttpException.ofCode(res.getCode()));
						return Promise.of(json != null ? fromJson(json, body.getString(UTF_8)) : null);
					} catch (ParseException e) {
						return Promise.ofException(e);
					} finally {
						body.recycle();
					}
				});
	}

}
