package io.global.pm.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsParser;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.Message;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class GlobalPmDriverServlet {
	public static final ParseException KEY_REQUIRED = new ParseException(GlobalPmDriverServlet.class, "Cookie 'Key' is required");

	public static <T> AsyncServlet create(GlobalPmDriver<T> driver) {
		StructuredCodec<Message<T>> messageCodec = getMessageCodec(driver.getPayloadCodec());
		ByteBufsParser<T> ndJsonPayloadParser = ndJsonParser(driver.getPayloadCodec());
		return RoutingServlet.create()
				.map(POST, "/" + SEND + "/:receiver/:mailbox", loadBody().serve(
						request -> {
							try {
								String receiverParameter = request.getPathParameter("receiver");
								String mailBox = request.getPathParameter("mailbox");
								PrivKey sender = request.getAttachment(PrivKey.class);
								PubKey receiver = PubKey.fromString(receiverParameter);
								ByteBuf body = request.getBody();
								T payload = fromJson(driver.getPayloadCodec(), body.getString(UTF_8));
								return driver.send(sender, receiver, mailBox, payload)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/" + POLL + "/:mailbox", request -> {
					String mailBox = request.getPathParameter("mailbox");
					KeyPair keys = request.getAttachment(PrivKey.class).computeKeys();
					return driver.poll(keys, mailBox)
							.map(message -> HttpResponse.ok200()
									.withJson(messageCodec.nullable(), message));
				})
				.map(DELETE, "/" + DROP + "/:mailbox/:id", request -> {
					try {
						String idParameter = request.getPathParameter("id");
						String mailBox = request.getPathParameter("mailbox");
						long id = Long.parseLong(idParameter);
						KeyPair keys = request.getAttachment(PrivKey.class).computeKeys();
						return driver.drop(keys, mailBox, id)
								.map($ -> HttpResponse.ok200());
					} catch (NumberFormatException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + MULTIPOLL + "/:mailbox", request -> {
					String key = request.getCookie("Key");
					KeyPair keys = request.getAttachment(PrivKey.class).computeKeys();
					String mailBox = request.getPathParameter("mailbox");
					long timestamp;
					try {
						String timestampParam = request.getQueryParameter("timestamp");
						timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
					} catch (NumberFormatException e) {
						return Promise.ofException(new ParseException(e));
					}
					return driver.multipoll(keys, mailBox, timestamp)
							.map(supplier -> HttpResponse.ok200()
									.withBodyStream(supplier.map(signedMessage -> toNdJsonBuf(messageCodec, signedMessage))));
				})
				.map(GET, "/" + BATCHPOLL + "/:mailbox", request -> {
					KeyPair keys = request.getAttachment(PrivKey.class).computeKeys();
					String mailBox = request.getPathParameter("mailbox");
					long timestamp;
					try {
						String timestampParam = request.getQueryParameter("timestamp");
						timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
					} catch (NumberFormatException e) {
						return Promise.ofException(new ParseException(e));
					}
					return driver.batchpoll(keys, mailBox, timestamp)
							.map(messages -> HttpResponse.ok200()
									.withJson(ofList(messageCodec), messages));
				})
				.map(POST, "/" + MULTISEND + "/:receiver/:mailbox", request -> {
					try {
						PrivKey sender = request.getAttachment(PrivKey.class);
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
				.map(DELETE, "/" + MULTIDROP + "/:mailbox", request -> {
					KeyPair keys = request.getAttachment(PrivKey.class).computeKeys();
					String mailBox = request.getPathParameter("mailbox");
					ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
					return driver.multidrop(keys, mailBox)
							.then(consumer -> BinaryChannelSupplier.of(bodyStream).parseStream(ND_JSON_ID_PARSER)
									.streamTo(consumer))
							.map($ -> HttpResponse.ok200());
				})
				.then(servlet ->
						request -> {
							try {
								String key = request.getCookie("Key");
								if (key == null) throw KEY_REQUIRED;
								request.attach(PrivKey.fromString(key));
								return servlet.serve(request);
							} catch (ParseException e) {
								return Promise.ofException(e);
							}

						});
	}
}
