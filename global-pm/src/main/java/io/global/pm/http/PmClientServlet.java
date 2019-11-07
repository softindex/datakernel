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
import io.global.common.PubKey;
import io.global.pm.api.Message;
import io.global.pm.api.PmClient;

import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.*;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PmClientServlet {

	public static <T> AsyncServlet create(PmClient<T> client, StructuredCodec<T> payloadCodec) {
		StructuredCodec<Message<T>> messageCodec = getMessageCodec(payloadCodec);
		ByteBufsParser<T> ndJsonPayloadParser = ndJsonParser(payloadCodec);
		return RoutingServlet.create()
				.map(POST, "/" + SEND + "/:receiver/:mailbox", loadBody().serve(
						request -> {
							try {
								PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
								String mailbox = request.getPathParameter("mailbox");
								T payload = fromJson(payloadCodec, request.getBody().asString(UTF_8));
								return client.send(receiver, mailbox, payload)
										.map(id -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.map(GET, "/" + POLL + "/:mailbox", request -> {
					String mailbox = request.getPathParameter("mailbox");
					return client.poll(mailbox)
							.map(message -> HttpResponse.ok200()
									.withJson(messageCodec.nullable(), message));
				})
				.map(DELETE, "/" + DROP + "/:mailbox/:id", request -> {
					try {
						String mailbox = request.getPathParameter("mailbox");
						long id = Long.parseLong(request.getPathParameter("id"));
						return client.drop(mailbox, id)
								.map($ -> HttpResponse.ok200());
					} catch (NumberFormatException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + MULTIPOLL + "/:mailbox", request -> {
						String mailBox = request.getPathParameter("mailbox");
						long timestamp;
						try {
							String timestampParam = request.getQueryParameter("timestamp");
							timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
						} catch (NumberFormatException e) {
							return Promise.ofException(new ParseException(e));
						}
						return client.multipoll(mailBox, timestamp)
								.map(supplier -> HttpResponse.ok200()
										.withBodyStream(supplier.map(signedMessage -> toNdJsonBuf(messageCodec, signedMessage))));
				})
				.map(GET, "/" + BATCHPOLL + "/:mailbox", request -> {
						String mailBox = request.getPathParameter("mailbox");
						long timestamp;
						try {
							String timestampParam = request.getQueryParameter("timestamp");
							timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
						} catch (NumberFormatException e) {
							return Promise.ofException(new ParseException(e));
						}
						return client.batchpoll(mailBox, timestamp)
								.map(messages -> HttpResponse.ok200()
										.withJson(ofList(messageCodec), messages));
				})
				.map(POST, "/" + MULTISEND + "/:receiver/:mailbox", request -> {
					try {
						PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
						String mailBox = request.getPathParameter("mailbox");
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return client.multisend(receiver, mailBox)
								.then(consumer -> BinaryChannelSupplier.of(bodyStream).parseStream(ndJsonPayloadParser)
										.streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(DELETE, "/" + MULTIDROP + "/:mailbox", request -> {
						String mailBox = request.getPathParameter("mailbox");
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return client.multidrop(mailBox)
								.then(consumer -> BinaryChannelSupplier.of(bodyStream).parseStream(ND_JSON_ID_PARSER)
										.streamTo(consumer))
								.map($ -> HttpResponse.ok200());
				});
	}

}
