package io.global.ot.service.messaging;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.util.ApplicationSettings;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.ot.service.UserContainerHolder;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.Utils.PUB_KEY_HEX_CODEC;
import static io.global.Utils.generateString;
import static io.global.ot.service.messaging.MessagingService.NO_RESOURCE_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

public final class MessagingServlet {
	public static final int RESOURCE_ID_LENGTH = ApplicationSettings.getInt(MessagingServlet.class, "resource.id.length", 32);
	public static final ParseException KEY_REQUIRED = new ParseException(MessagingServlet.class, "Cookie 'Key' is required");

	public static RoutingServlet create(UserContainerHolder<?> userContainerHolder) {
		return RoutingServlet.create()
				.with(POST, "/add", loadBody().serve(handleAdd(userContainerHolder, false)))
				.with(POST, "/addUnique", loadBody().serve(handleAdd(userContainerHolder, true)))
				.with(POST, "/delete", loadBody().serve(request -> {
					try {
						String key = request.getCookie("Key");
						if (key == null) {
							throw KEY_REQUIRED;
						}
						PrivKey privKey = PrivKey.fromString(key);
						String id = fromJson(STRING_CODEC, request.getBody().getString(UTF_8));
						return userContainerHolder.ensureUserContainer(privKey)
								.then(userContainer -> userContainer.getMessagingService().delete(id))
								.thenEx(($, e) -> {
									if (e == null) {
										return Promise.of(HttpResponse.ok200());
									} else if (e == NO_RESOURCE_FOUND) {
										return Promise.of(HttpResponse.ofCode(404).withBody(e.getMessage().getBytes(UTF_8)));
									} else {
										return Promise.<HttpResponse>ofException(e);
									}
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				}));

	}

	@NotNull
	private static AsyncServlet handleAdd(UserContainerHolder<?> userContainerHolder, boolean unique) {
		return request -> {
					try {
						String key = request.getCookie("Key");
						if (key == null) {
							throw KEY_REQUIRED;
						}
						PrivKey privKey = PrivKey.fromString(key);
						Set<PubKey> participants = fromJson(ofSet(PUB_KEY_HEX_CODEC), request.getBody().getString(UTF_8));
						participants.add(privKey.computePubKey());
						assert participants.size() >= 2 : "Wrong number of participants";
						String id = unique ?
								participants.stream().map(PubKey::asString).collect(joining("&")) :
								generateString(RESOURCE_ID_LENGTH);
						return userContainerHolder.ensureUserContainer(privKey)
								.then(userContainer -> userContainer.getMessagingService().sendCreateMessage(id, participants))
								.map($ -> HttpResponse.ok200().withBody(id.getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
		};
	}

}
