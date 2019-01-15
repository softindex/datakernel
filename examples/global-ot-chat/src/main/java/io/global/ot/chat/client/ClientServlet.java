package io.global.ot.chat.client;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.global.ot.chat.operations.ChatOTState.ChatEntry;

import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.http.HttpMethod.*;
import static java.lang.Long.parseLong;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ClientServlet implements WithMiddleware {
	private final MiddlewareServlet servlet;
	private final StructuredCodec<ChatEntry> CHAT_ENTRY_CODEC = StructuredCodecs.tuple(ChatEntry::new,
			ChatEntry::getTimestamp, StructuredCodecs.LONG_CODEC,
			ChatEntry::getContent, StructuredCodecs.STRING_CODEC);

	private ClientServlet(ChatStateManager stateManager) {
		this.servlet = getServlet(stateManager);
	}

	public static ClientServlet create(ChatStateManager stateManager) {
		return new ClientServlet(stateManager);
	}

	private MiddlewareServlet getServlet(ChatStateManager stateManager) {
		return MiddlewareServlet.create()
				.with(POST, "/send", request -> request.getBody()
						.thenCompose(body -> {
							try {
								return stateManager.sendOperation(System.currentTimeMillis(), body.getString(UTF_8), false)
										.thenApply($ -> HttpResponse.ok200());
							} finally {
								body.recycle();
							}
						}))
				.with(DELETE, "/delete/:timestamp*", request -> request.getBody()
						.thenCompose(body -> {
							try {
								return stateManager.sendOperation(parseLong(request.getPathParameter("timestamp")), body.getString(UTF_8), true)
										.thenApply($ -> HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.<HttpResponse>ofException(e);
							} finally {
								body.recycle();
							}
						}))
				.with(GET, "/update", request -> stateManager.getState()
						.thenApply(state -> HttpResponse.ok200()
								.withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON)))
								.withBody(JsonUtils.toJson(ofSet(CHAT_ENTRY_CODEC), state).getBytes(UTF_8))));
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
}
