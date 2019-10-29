package io.global.ot.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.ot.OTUplink;
import io.datakernel.promise.Promise;
import io.global.ot.api.CommitId;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpRequest.get;
import static io.datakernel.http.HttpRequest.post;
import static io.datakernel.http.UrlBuilder.urlEncode;
import static io.global.ot.api.OTUplinkCommand.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OTUplinkHttpClient<K, D> implements OTUplink<K, D, byte[]> {
	private final IAsyncHttpClient httpClient;
	private final String url;
	private final StructuredCodec<K> revisionCodec;
	private final StructuredCodec<FetchData<K, D>> fetchDataCodec;

	private OTUplinkHttpClient(IAsyncHttpClient httpClient, String url, StructuredCodec<K> revisionCodec, StructuredCodec<D> diffCodec) {
		this.httpClient = httpClient;
		this.url = url.endsWith("/") ? url : url + '/';
		this.revisionCodec = revisionCodec;
		fetchDataCodec = FetchData.codec(revisionCodec, diffCodec);
	}

	public static <K, D> OTUplinkHttpClient<K, D> create(IAsyncHttpClient httpClient, String url, StructuredCodec<K> revisionCodec, StructuredCodec<D> diffCodec) {
		return new OTUplinkHttpClient<>(httpClient, url, revisionCodec, diffCodec);
	}

	public static <D> OTUplinkHttpClient<CommitId, D> forGlobalNode(IAsyncHttpClient httpClient, String url,
																  StructuredCodec<D> diffCodec) {
		return new OTUplinkHttpClient<>(httpClient, url, REGISTRY.get(CommitId.class), diffCodec);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Promise<byte[]> createProtoCommit(K parent, List<D> diffs, long parentLevel) {
		FetchData<K, D> fetchData = new FetchData<>(parent, parentLevel, diffs);
		return httpClient.request(post(url + CREATE_COMMIT)
				.withBody(toJson(fetchDataCodec, fetchData).getBytes(UTF_8)))
				.then(response -> response.loadBody()
						.map(ByteBuf::asArray));
	}

	@Override
	public Promise<FetchData<K, D>> push(byte[] commit) {
		return httpClient.request(post(url + PUSH)
				.withBody(commit))
				.then(response -> response.loadBody()
						.then(body -> processResult(response, body, fetchDataCodec)));
	}

	@Override
	public Promise<FetchData<K, D>> checkout() {
		return httpClient.request(get(url + CHECKOUT))
				.then(response -> response.loadBody()
						.then(body -> processResult(response, body, fetchDataCodec)));
	}

	@Override
	public Promise<FetchData<K, D>> fetch(K currentCommitId) {
		return httpClient.request(get(url + FETCH + "?id=" + urlEncode(toJson(revisionCodec, currentCommitId))))
				.then(response -> response.loadBody()
						.then(body -> processResult(response, body, fetchDataCodec)));
	}

	private static <T> Promise<T> processResult(HttpResponse res, ByteBuf body, @NotNull StructuredCodec<T> json) {
		try {
			if (res.getCode() != 200) {
				return Promise.ofException(HttpException.ofCode(res.getCode()));
			}
			return Promise.of(fromJson(json, body.getString(UTF_8)));
		} catch (ParseException e) {
			return Promise.ofException(HttpException.ofCode(400, e));
		}
	}
}
