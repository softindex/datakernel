package io.global.pm.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static io.datakernel.codec.binary.BinaryUtils.encodeWithSizePrefix;
import static io.global.pm.http.GlobalPmNodeServlet.STRING_SET_CODEC;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.BinaryDataFormats.SIGNED_RAW_MSG_CODEC;
import static io.global.pm.util.BinaryDataFormats.SIGNED_RAW_MSG_PARSER;

public class HttpGlobalPmNode implements GlobalPmNode {
	private static final String PM_NODE_SUFFIX = "/pm/";

	private final String url;
	private final IAsyncHttpClient client;

	private HttpGlobalPmNode(String url, IAsyncHttpClient client) {
		this.url = url + PM_NODE_SUFFIX;
		this.client = client;
	}

	public static HttpGlobalPmNode create(String url, IAsyncHttpClient client) {
		return new HttpGlobalPmNode(url, client);
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawMessage>>> upload(PubKey space, String mailBox) {
		ChannelZeroBuffer<SignedData<RawMessage>> buffer = new ChannelZeroBuffer<>();
		Promise<HttpResponse> request = client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(UPLOAD)
								.appendPathPart(space.asString())
								.appendPathPart(mailBox)
								.build())
						.withBodyStream(buffer.getSupplier()
								.map(signedDbItem -> encodeWithSizePrefix(SIGNED_RAW_MSG_CODEC, signedDbItem))))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response));
		return Promise.of(buffer.getConsumer().withAcknowledgement(ack -> ack.both(request)));

	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(DOWNLOAD)
								.appendPathPart(space.asString())
								.appendPathPart(mailBox)
								.appendQuery("timestamp", timestamp)
								.build()))
				.then(response -> response.getCode() != 200 ?
						Promise.ofException(HttpException.ofCode(response.getCode())) : Promise.of(response))
				.map(response -> BinaryChannelSupplier.of(response.getBodyStream()).parseStream(SIGNED_RAW_MSG_PARSER));
	}

	@NotNull
	@Override
	public Promise<Void> send(PubKey space, String mailBox, SignedData<RawMessage> message) {
		return client.request(HttpRequest.post(url +
				UrlBuilder.relative()
						.appendPathPart(SEND)
						.appendPathPart(space.asString())
						.appendPathPart(mailBox)
						.build())
				.withBody(BinaryUtils.encode(SIGNED_RAW_MSG_CODEC, message)))
				.then(response -> response.getCode() == 200 ?
						Promise.complete() :
						Promise.ofException(HttpException.ofCode(response.getCode())))
				.toVoid();
	}

	@NotNull
	@Override
	public Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox) {
		return client.request(HttpRequest.get(url +
				UrlBuilder.relative()
						.appendPathPart(POLL)
						.appendPathPart(space.asString())
						.appendPathPart(mailBox)
						.build()))
				.then(response -> response.loadBody()
						.then(body -> processResult(response, body, SIGNED_RAW_MSG_CODEC.nullable())));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return client.request(HttpRequest.get(url + UrlBuilder.relative()
				.appendPathPart(LIST)
				.appendPathPart(space.asString())
				.build()))
				.then(response -> response.loadBody()
						.then(body -> processResult(response, body, STRING_SET_CODEC)));
	}

	private static <T> Promise<T> processResult(HttpResponse res, ByteBuf body, @Nullable StructuredCodec<T> codec) {
		try {
			if (res.getCode() != 200) {
				return Promise.ofException(HttpException.ofCode(res.getCode()));
			}
			return Promise.of(codec != null ? BinaryUtils.decode(codec, body.slice()) : null);
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}
}
