package io.global.ot.chat.client;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.http.*;
import io.datakernel.ot.OTStateManager;
import io.global.ot.api.CommitId;
import io.global.ot.chat.operations.ChatOTState;
import io.global.ot.chat.operations.ChatOTState.ChatEntry;
import io.global.ot.chat.operations.ChatOperation;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.ot.chat.operations.ChatOperation.delete;
import static io.global.ot.chat.operations.ChatOperation.insert;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class ClientServlet implements WithMiddleware {
	private final MiddlewareServlet servlet;
	private final StructuredCodec<ChatEntry> CHAT_ENTRY_CODEC = StructuredCodecs.tuple(ChatEntry::new,
			ChatEntry::getTimestamp, StructuredCodecs.LONG_CODEC,
			ChatEntry::getAuthor, StructuredCodecs.STRING_CODEC,
			ChatEntry::getContent, StructuredCodecs.STRING_CODEC);

	private ClientServlet(OTStateManager<CommitId, ChatOperation> stateManager) {
		this.servlet = getServlet(stateManager);
	}

	public static ClientServlet create(OTStateManager<CommitId, ChatOperation> stateManager) {
		return new ClientServlet(stateManager);
	}

	private MiddlewareServlet getServlet(OTStateManager<CommitId, ChatOperation> stateManager) {
		return MiddlewareServlet.create()
				.with(POST, "/send", request -> request.getPostParameters()
						.thenCompose(postParameters -> {
							String content = postParameters.get("content");
							String author = postParameters.get("author");
							stateManager.add(insert(System.currentTimeMillis(), author, content));
							return Promise.of(HttpResponse.ok200());
						}))
				.with(POST, "/delete", request -> request.getPostParameters()
						.thenCompose(postParameters -> {
							try {
								String author = postParameters.get("author");
								long timestamp = parseLong(postParameters.get("timestamp"));
								String content = postParameters.get("content");
								stateManager.add(delete(timestamp, author, content));
								return Promise.of(HttpResponse.ok200());
							} catch (NumberFormatException e) {
								return Promise.<HttpResponse>ofException(e);
							}
						}))
				.with(GET, "/update", request -> {
					Set<ChatEntry> chatEntries = ((ChatOTState) stateManager.getState()).getChatEntries();
					return Promise.of(HttpResponse.ok200()
							.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON)))
							.withBody(JsonUtils.toJson(ofSet(CHAT_ENTRY_CODEC), chatEntries).getBytes(UTF_8)));
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
