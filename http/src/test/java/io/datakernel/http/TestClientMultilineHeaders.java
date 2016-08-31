package io.datakernel.http;

import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestClientMultilineHeaders {

	public static final int PORT = 9595;

	@Test
	public void testMultilineHeaders() throws ExecutionException, InterruptedException, IOException {
		Eventloop eventloop = new Eventloop();
		final AsyncHttpClient httpClient = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, 3_000L, HttpUtils.inetAddress("8.8.8.8")));

		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		final AsyncHttpServer server = new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				callback.onResult(HttpResponse.create().header(HttpHeaders.ALLOW, "GET,\r\n HEAD"));
			}
		});

		server.setListenPort(PORT);
		server.listen();

		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(result.getHeader(HttpHeaders.ALLOW));
				httpClient.close();
				server.close();
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.onException(exception);
				httpClient.close();
				server.close();
			}
		});

		eventloop.run();
		assertEquals("GET,   HEAD", resultObserver.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}
}
