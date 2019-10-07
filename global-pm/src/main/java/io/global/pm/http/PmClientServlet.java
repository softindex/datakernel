package io.global.pm.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.PubKey;
import io.global.pm.api.Message;
import io.global.pm.api.PmClient;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.HttpMethod.*;
import static io.global.pm.http.PmCommand.*;
import static io.global.pm.util.HttpDataFormats.getMessageCodec;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PmClientServlet {
	static CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	public static <T> AsyncServlet create(PmClient<T> client, StructuredCodec<T> payloadCodec) {
		StructuredCodec<Message<T>> messageCodec = getMessageCodec(payloadCodec);
		return RoutingServlet.create()
				.map(POST, "/" + SEND + "/:receiver/:mailbox", request -> {
					try {
						PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
						String mailbox = request.getPathParameter("mailbox");
						T payload = fromJson(payloadCodec, request.getBody().asString(UTF_8));
						return client.send(receiver, mailbox, now.currentTimeMillis(), payload)
								.map(id -> HttpResponse.ok200()
										.withPlainText(id.toString()));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/" + POLL + "/:mailbox", request -> {
					String mailbox = request.getPathParameter("mailbox");
					//noinspection ConstantConditions - codec is nullable()
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
				.then(AsyncServletDecorator.loadBody());
	}

}
