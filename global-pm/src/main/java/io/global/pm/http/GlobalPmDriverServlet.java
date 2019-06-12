package io.global.pm.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.Message;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.global.pm.http.PmCommand.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class GlobalPmDriverServlet {
	public static final ParseException KEY_REQUIRED = new ParseException(GlobalPmDriverServlet.class, "Cookie 'Key' is required");

	public static <T> RoutingServlet create(GlobalPmDriver<T> driver, StructuredCodec<Message<T>> codec) {
		return RoutingServlet.create()
				.with(POST, "/" + SEND + "/:receiver/:mailbox", loadBody().serve(
						request -> {
							try {
								String key = request.getCookie("Key");
								String receiverParameter = checkNotNull(request.getPathParameter("receiver"));
								String mailBox = checkNotNull(request.getPathParameter("mailbox"));
								if (key == null) throw KEY_REQUIRED;
								PrivKey sender = PrivKey.fromString(key);
								PubKey receiver = PubKey.fromString(receiverParameter);
								ByteBuf body = request.getBody();
								Message<T> message = fromJson(codec, body.getString(UTF_8));
								return driver.send(sender, receiver, mailBox, message)
										.map($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
				.with(GET, "/" + POLL + "/:mailbox", request -> {
					try {
						String key = request.getCookie("Key");
						String mailBox = checkNotNull(request.getPathParameter("mailbox"));
						if (key == null) throw KEY_REQUIRED;
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						return driver.poll(keys, mailBox)
								.map(message -> HttpResponse.ok200()
										.withJson(codec.nullable(), message));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/" + DROP + "/:mailbox/:id", request -> {
					try {
						String key = request.getCookie("Key");
						String idParameter = checkNotNull(request.getPathParameter("id"));
						String mailBox = checkNotNull(request.getPathParameter("mailbox"));
						if (key == null) throw KEY_REQUIRED;
						long id = Long.parseLong(idParameter);
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						return driver.drop(keys, mailBox, id)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException | NumberFormatException e) {
						return Promise.ofException(e);
					}
				});
	}
}
