package io.datakernel.http;

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertEquals;

public class TestClientMultilineHeaders {

	public static final int PORT = 9595;
	public static final InetAddress GOOGLE_PUBLIC_DNS = HttpUtils.inetAddress("8.8.8.8");

	@Test
	public void testMultilineHeaders() throws ExecutionException, InterruptedException, IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);

		AsyncServlet servlet = request -> {
			HttpResponse response = HttpResponse.ok200();
			response.addHeader(HttpHeaders.ALLOW, "GET,\r\n HEAD");
			return SettableStage.immediateStage(response);
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenAddress(new InetSocketAddress("localhost", PORT));
		server.listen();

		final CompletableFuture<String> future = httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT))
				.thenApply(response -> {
					httpClient.stop();
					server.close();
					return response.getHeader(HttpHeaders.ALLOW);
				}).toCompletableFuture();

		eventloop.run();
		assertEquals("GET,   HEAD", future.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
