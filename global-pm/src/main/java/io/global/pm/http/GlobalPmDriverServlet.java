package io.global.pm.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.Message;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.oneline;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class GlobalPmDriverServlet {
	public static final ParseException KEY_REQUIRED = new ParseException(GlobalPmDriverServlet.class, "Cookie 'Key' is required");
	public static final ByteBufsParser<Long> ND_JSON_ID_PARSER = ByteBufsParser.ofLfTerminatedBytes()
			.andThen(value -> fromJson(LONG_CODEC, value.asString(UTF_8)));

	public static <T> RoutingServlet create(GlobalPmDriver<T> driver, StructuredCodec<T> payloadCodec) {
		StructuredCodec<T> codec = oneline(payloadCodec);
		StructuredCodec<Message<T>> messageCodec = getMessageCodec(codec);
		ByteBufsParser<T> ndJsonPayloadParser = ndJsonParser(codec);
		return RoutingServlet.create()
				.map(POST, "/" + SEND + "/:receiver/:mailbox", loadBody().serve(
						request -> {
							try {
								String key = request.getCookie("Key");
								String receiverParameter = request.getPathParameter("receiver");
								String mailBox = request.getPathParameter("mailbox");
								if (key == null) throw KEY_REQUIRED;
								PrivKey sender = PrivKey.fromString(key);
								PubKey receiver = PubKey.fromString(receiverParameter);
								ByteBuf body = request.getBody();
								T payload = fromJson(codec, body.getString(UTF_8));
								return driver.send(sender, receiver, mailBox, payload)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/" + POLL + "/:mailbox", request -> {
					try {
						String key = request.getCookie("Key");
						String mailBox = request.getPathParameter("mailbox");
						if (key == null) throw KEY_REQUIRED;
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						return driver.poll(keys, mailBox)
								.map(message -> HttpResponse.ok200()
										.withJson(messageCodec.nullable(), message));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(POST, "/" + DROP + "/:mailbox/:id", request -> {
					try {
						String key = request.getCookie("Key");
						String idParameter = request.getPathParameter("id");
						String mailBox = request.getPathParameter("mailbox");
						if (key == null) throw KEY_REQUIRED;
						long id = Long.parseLong(idParameter);
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						return driver.drop(keys, mailBox, id)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException | NumberFormatException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + MULTIPOLL + "/:mailbox", request -> {
					try {
						String key = request.getCookie("Key");
						if (key == null) throw KEY_REQUIRED;
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						String mailBox = request.getPathParameter("mailbox");
						long timestamp;
						try {
							String timestampParam = request.getQueryParameter("timestamp");
							timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return driver.multipoll(keys, mailBox, timestamp)
								.map(supplier -> HttpResponse.ok200()
										.withBodyStream(supplier.map(signedMessage -> toNdJsonBuf(messageCodec, signedMessage))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(POST, "/" + MULTISEND + "/:receiver/:mailbox", request -> {
					try {
						String key = request.getCookie("Key");
						if (key == null) throw KEY_REQUIRED;
						PrivKey sender = PrivKey.fromString(key);
						PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
						String mailBox = request.getPathParameter("mailbox");
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return driver.multisend(sender, receiver, mailBox)
								.then(consumer -> BinaryChannelSupplier.of(bodyStream).parseStream(ndJsonPayloadParser)
										.streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(POST, "/" + MULTIDROP + "/:mailbox", request -> {
					try {
						String key = request.getCookie("Key");
						if (key == null) throw KEY_REQUIRED;
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						String mailBox = request.getPathParameter("mailbox");
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return driver.multidrop(keys, mailBox)
								.then(consumer -> BinaryChannelSupplier.of(bodyStream).parseStream(ND_JSON_ID_PARSER)
										.streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}
}
