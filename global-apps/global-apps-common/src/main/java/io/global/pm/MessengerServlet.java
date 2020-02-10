package io.global.pm;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.common.PubKey;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.AsyncServletDecorator.onRequest;
import static io.datakernel.http.HttpMethod.*;
import static io.global.pm.PmUtils.getMessageCodec;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class MessengerServlet {
	public static <K, V> AsyncServlet create(Messenger<K, V> messenger) {
		StructuredCodec<Message<K, V>> messageCodec = getMessageCodec(messenger.getKeyCodec(), messenger.getValueCodec());
		return RoutingServlet.create()
				.map(POST, "/send/:receiver/:mailbox", request -> {
					try {
						PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
						String mailbox = request.getPathParameter("mailbox");
						V payload = fromJson(messenger.getValueCodec(), request.getBody().getString(UTF_8));
						KeyPair keys = request.getAttachment(KeyPair.class);
						return messenger.send(keys, receiver, mailbox, payload)
								.map(id -> HttpResponse.ok200()
										.withJson(JsonUtils.toJson(messenger.getKeyCodec(), id)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.map(GET, "/poll/:mailbox", request -> {
					String mailbox = request.getPathParameter("mailbox");
					KeyPair keys = request.getAttachment(KeyPair.class);
					return messenger.poll(keys, mailbox)
							.map(message -> HttpResponse.ok200()
									.withJson(JsonUtils.toJson(messageCodec.nullable(), message)));
				})
				.map(DELETE, "/drop/:mailbox", request -> {
					try {
						String mailbox = request.getPathParameter("mailbox");
						K id = fromJson(messenger.getKeyCodec(), request.getBody().getString(UTF_8));
						KeyPair keys = request.getAttachment(KeyPair.class);
						return messenger.drop(keys, mailbox, id)
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.then(onRequest(request -> checkState(request.getAttachmentKeys().contains(KeyPair.class), "Key pair should be attached to a request")))
				.then(loadBody());
	}
}
