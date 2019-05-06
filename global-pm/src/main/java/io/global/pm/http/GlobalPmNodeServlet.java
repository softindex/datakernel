package io.global.pm.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.BinaryDataFormats.*;

public class GlobalPmNodeServlet implements WithMiddleware {
	private final MiddlewareServlet servlet;

	private GlobalPmNodeServlet(GlobalPmNode node) {
		this.servlet = servlet(node);
	}

	public static GlobalPmNodeServlet create(GlobalPmNode node) {
		return new GlobalPmNodeServlet(node);
	}

	private MiddlewareServlet servlet(GlobalPmNode node) {
		return MiddlewareServlet.create()
				.with(POST, "/" + SEND + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						return request.getBody()
								.then(body -> {
									try {
										SignedData<RawMessage> message = BinaryUtils.decode(SIGNED_RAW_MSG_CODEC, body);
										return node.send(space, mailBox, message)
												.map($ -> HttpResponse.ok200());
									} catch (ParseException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + MULTISEND + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
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
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						return node.poll(space, mailBox)
								.map(message -> HttpResponse.ok200()
										.withBody(BinaryUtils.encode(SIGNED_RAW_MSG_CODEC.nullable(), message)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + MULTIPOLL + "/:space/:mailbox", request -> {
					try {
						ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();

						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
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
				.with(POST, "/" + DROP + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						return request.getBody()
								.then(body -> {
									try {
										SignedData<Long> id = BinaryUtils.decode(SIGNED_LONG_CODEC, body);
										return node.drop(space, mailBox, id)
												.map($ -> HttpResponse.ok200());
									} catch (ParseException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + MULTIDROP + "/:space/:mailbox", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						String mailBox = request.getPathParameter("mailbox");
						return BinaryChannelSupplier.of(request.getBodyStream())
								.parseStream(SIGNED_LONG_PARSER)
								.streamTo(node.multidrop(space, mailBox))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
