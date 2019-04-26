package io.global.pn.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.pn.GlobalPmDriver;
import io.global.pn.api.Message;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.pn.http.PmCommand.*;
import static io.global.pn.util.HttpDataFormats.getMessageCodec;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class GlobalPmDriverServlet<T> implements WithMiddleware {
	private final MiddlewareServlet servlet;

	public GlobalPmDriverServlet(GlobalPmDriver<T> driver, StructuredCodec<T> payloadCodec) {
		this.servlet = getServlet(driver, getMessageCodec(payloadCodec));
	}

	private static <T> MiddlewareServlet getServlet(GlobalPmDriver<T> driver, StructuredCodec<Message<T>> codec) {
		return MiddlewareServlet.create()
				.with(POST, "/" + SEND + "/:receiver/:mailbox", request -> request.getBody()
						.then(body -> {
							try {
								PrivKey sender = PrivKey.fromString(request.getCookie("Key"));
								PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
								String mailBox = request.getPathParameter("mailbox");
								Message<T> message = fromJson(codec, body.getString(UTF_8));
								return driver.send(sender, receiver, mailBox, message)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.<HttpResponse>ofException(e);
							} finally {
								body.recycle();
							}
						}))
				.with(GET, "/" + POLL + "/:mailbox", request -> {
					try {
						KeyPair keys = PrivKey.fromString(request.getCookie("Key")).computeKeys();
						String mailBox = request.getPathParameter("mailbox");
						return driver.poll(keys, mailBox)
								.map(message -> HttpResponse.ok200()
										.withJson(codec.nullable(), message));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + DROP + "/:mailbox/:id", request -> {
					try {
						long id = Long.parseLong(request.getPathParameter("id"));
						KeyPair keys = PrivKey.fromString(request.getCookie("Key")).computeKeys();
						String mailBox = request.getPathParameter("mailbox");
						return driver.drop(keys, mailBox, id)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException | NumberFormatException e) {
						return Promise.ofException(e);
					}
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
