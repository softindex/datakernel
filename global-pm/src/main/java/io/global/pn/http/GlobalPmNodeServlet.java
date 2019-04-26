package io.global.pn.http;

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
import io.global.pn.api.GlobalPmNode;
import io.global.pn.api.RawMessage;

import java.util.Arrays;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.pn.http.PmCommand.*;
import static io.global.pn.util.BinaryDataFormats.*;

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
				.with(POST, "/" + SEND + "/:space", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						return request.getBody()
								.then(body -> {
									try {
										SignedData<RawMessage> message = BinaryUtils.decode(SIGNED_RAW_MSG_CODEC, body);
										return node.send(space, message)
												.map($ -> HttpResponse.ok200());
									} catch (ParseException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + MULTISEND + "/:space", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						return BinaryChannelSupplier.of(request.getBodyStream())
								.parseStream(SIGNED_RAW_MSG_PARSER)
								.streamTo(node.multisend(space))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + POLL + "/:space", request -> {
					try {
						return node.poll(PubKey.fromString(request.getPathParameter("space")))
								.map(message -> HttpResponse.ok200()
										.withBody(BinaryUtils.encode(SIGNED_RAW_MSG_CODEC.nullable(), message)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/" + MULTIPOLL + "/:space", request -> {
					try {
						ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();

						Promise<Void> process = node.multipoll(PubKey.fromString(request.getPathParameter("space")))
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
				.with(POST, "/" + DROP + "/:space", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						return request.getBody()
								.then(body -> {
									try {
										System.out.println("decoding " + Arrays.toString(body.getArray()));
										SignedData<Long> id = BinaryUtils.decode(SIGNED_LONG_CODEC, body);
										return node.drop(space, id)
												.map($ -> HttpResponse.ok200());
									} catch (ParseException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								});
					} catch(ParseException e){
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + MULTIDROP + "/:space", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						return BinaryChannelSupplier.of(request.getBodyStream())
								.parseStream(SIGNED_LONG_PARSER)
								.streamTo(node.multidrop(space))
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
