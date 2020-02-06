package io.global.pm.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.binary.BinaryUtils.encodeWithSizePrefix;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.BinaryDataFormats.SIGNED_RAW_MSG_CODEC;
import static io.global.pm.util.BinaryDataFormats.SIGNED_RAW_MSG_PARSER;

public class GlobalPmNodeServlet {
	public static final StructuredCodec<Set<String>> STRING_SET_CODEC = StructuredCodecs.ofSet(STRING_CODEC);

	public static RoutingServlet create(GlobalPmNode node) {
		return RoutingServlet.create()
				.map(POST, "/" + SEND + "/:space/:mailbox", loadBody()
						.serve(request -> {
							try {
								String spacePathParameter = request.getPathParameter("space");
								String mailBox = request.getPathParameter("mailbox");
								PubKey space = PubKey.fromString(spacePathParameter);
								SignedData<RawMessage> message = BinaryUtils.decode(SIGNED_RAW_MSG_CODEC, request.getBody().getArray());
								return node.send(space, mailBox, message)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						})
				)
				.map(GET, "/" + POLL + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						return node.poll(space, mailBox)
								.map(message -> HttpResponse.ok200()
										.withBody(BinaryUtils.encode(SIGNED_RAW_MSG_CODEC.nullable(), message)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + LIST + "/:space", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						return node.list(space)
								.map(mailBoxes -> HttpResponse.ok200()
										.withBody(BinaryUtils.encode(STRING_SET_CODEC, mailBoxes)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + DOWNLOAD + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						long timestamp;
						try {
							String timestampParam = request.getQueryParameter("timestamp");
							timestamp = Long.parseUnsignedLong(timestampParam != null ? timestampParam : "0");
						} catch (NumberFormatException e) {
							throw new ParseException(e);
						}
						return node.download(space, mailBox, timestamp)
								.map(supplier -> HttpResponse.ok200()
										.withBodyStream(supplier.map(signedMessage -> encodeWithSizePrefix(SIGNED_RAW_MSG_CODEC, signedMessage))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(POST, "/" + UPLOAD + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						ChannelSupplier<ByteBuf> bodyStream = request.getBodyStream();
						return node.upload(space, mailBox)
								.then(consumer -> BinaryChannelSupplier.of(bodyStream).parseStream(SIGNED_RAW_MSG_PARSER)
										.streamTo(consumer))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}
}
