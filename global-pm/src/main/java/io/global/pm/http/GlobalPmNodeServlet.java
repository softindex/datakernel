package io.global.pm.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.BinaryDataFormats.*;

public class GlobalPmNodeServlet {

	public static RoutingServlet create(GlobalPmNode node) {
		return RoutingServlet.create()
				.with(POST, "/" + SEND + "/:space/:mailbox", loadBody()
						.serve(request -> {
							try {
								String spacePathParameter = checkNotNull(request.getPathParameter("space"));
								String mailBox = checkNotNull(request.getPathParameter("mailbox"));
								PubKey space = PubKey.fromString(spacePathParameter);
								try {
									SignedData<RawMessage> message = BinaryUtils.decode(SIGNED_RAW_MSG_CODEC, request.getBody().getArray());
									return node.send(space, mailBox, message)
											.map($ -> HttpResponse.ok200());
								} catch (ParseException e) {
									return Promise.ofException(e);
								}
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						})
				)
				.with(POST, "/" + MULTISEND + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(checkNotNull(request.getPathParameter("space")));
						String mailBox = checkNotNull(request.getPathParameter("mailbox"));
						return BinaryChannelSupplier.of(request.getBodyStream())
								.parseStream(SIGNED_RAW_MSG_PARSER)
								.streamTo(node.multisend(space, mailBox))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + POLL + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(checkNotNull(request.getPathParameter("space")));
						String mailBox = request.getPathParameter("mailbox");
						return node.poll(space, checkNotNull(mailBox))
								.map(message -> HttpResponse.ok200()
										.withBody(BinaryUtils.encode(SIGNED_RAW_MSG_CODEC.nullable(), message)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + MULTIPOLL + "/:space/:mailbox", request -> {
					try {
						ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();

						PubKey space = PubKey.fromString(checkNotNull(request.getPathParameter("space")));
						String mailBox = checkNotNull(request.getPathParameter("mailbox"));
						Promise<Void> process = node.multipoll(space, mailBox)
								.then(supplier -> supplier
										.map(message -> BinaryUtils.encode(SIGNED_RAW_MSG_CODEC, message))
										.streamTo(buffer.getConsumer()));

						return Promise.of(HttpResponse.ok200()
								.withBodyStream(buffer.getSupplier()
										.withEndOfStream(eos -> eos.both(process))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + DROP + "/:space/:mailbox", loadBody()
						.serve(request -> {
							try {
								PubKey space = PubKey.fromString(checkNotNull(request.getPathParameter("space")));
								String mailBox = checkNotNull(request.getPathParameter("mailbox"));
								try {
									SignedData<Long> id = BinaryUtils.decode(SIGNED_LONG_CODEC, request.getBody().getArray());
									return node.drop(space, mailBox, id)
											.map($ -> HttpResponse.ok200());
								} catch (ParseException e) {
									return Promise.ofException(e);
								}
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(POST, "/" + MULTIDROP + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(checkNotNull(request.getPathParameter("space")));
						String mailBox = checkNotNull(request.getPathParameter("mailbox"));
						return BinaryChannelSupplier.of(request.getBodyStream())
								.parseStream(SIGNED_LONG_PARSER)
								.streamTo(node.multidrop(space, mailBox))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}
}
