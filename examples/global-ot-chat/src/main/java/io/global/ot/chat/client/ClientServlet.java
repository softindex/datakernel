package io.global.ot.chat.client;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.http.*;
import io.global.ot.chat.operations.ChatOTState;
import io.global.ot.chat.operations.ChatOTState.ChatEntry;
import io.global.ot.chat.operations.ChatOperation;
import io.global.ot.common.ManagerProvider;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.ot.chat.operations.ChatOperation.delete;
import static io.global.ot.chat.operations.ChatOperation.insert;
import static io.global.ot.chat.operations.Utils.getManager;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class ClientServlet implements WithMiddleware {
	private final MiddlewareServlet servlet;
	private final ManagerProvider<ChatOperation> managerProvider;
	private final StructuredCodec<ChatEntry> CHAT_ENTRY_CODEC = StructuredCodecs.tuple(ChatEntry::new,
			ChatEntry::getTimestamp, StructuredCodecs.LONG_CODEC,
			ChatEntry::getAuthor, StructuredCodecs.STRING_CODEC,
			ChatEntry::getContent, StructuredCodecs.STRING_CODEC);

	private ClientServlet(ManagerProvider<ChatOperation> managerProvider) {
		this.managerProvider = managerProvider;
		this.servlet = getServlet();
	}

	public static ClientServlet create(ManagerProvider<ChatOperation> managerProvider) {
		return new ClientServlet(managerProvider);
	}

	private MiddlewareServlet getServlet() {
		return MiddlewareServlet.create()
				.with(POST, "/send", request -> request.getPostParameters()
						.thenCompose(postParameters -> getManager(managerProvider, request)
								.thenCompose(manager -> {
									String content = postParameters.get("content");
									String author = postParameters.get("author");
									manager.add(insert(System.currentTimeMillis(), author, content));
									return Promise.of(HttpResponse.ok200());
								})))
				.with(POST, "/delete", request -> request.getPostParameters()
						.thenCompose(postParameters -> getManager(managerProvider, request)
								.thenCompose(manager -> {
									try {
										String author = postParameters.get("author");
										long timestamp = parseLong(postParameters.get("timestamp"));
										String content = postParameters.get("content");
										manager.add(delete(timestamp, author, content));
										return Promise.of(HttpResponse.ok200());
									} catch (NumberFormatException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								})))
				.with(GET, "/update", request -> getManager(managerProvider, request)
						.thenCompose(manager -> {
							Set<ChatEntry> chatEntries = ((ChatOTState) manager.getState()).getChatEntries();
							return Promise.of(HttpResponse.ok200()
									.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON)))
									.withBody(JsonUtils.toJson(ofSet(CHAT_ENTRY_CODEC), chatEntries).getBytes(UTF_8)));
						}));
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
