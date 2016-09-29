package io.datakernel.http;

import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestClientMultilineHeaders {

	public static final int PORT = 9595;
	public static final InetAddress GOOGLE_PUBLIC_DNS = HttpUtils.inetAddress("8.8.8.8");

	@Test
	public void testMultilineHeaders() throws ExecutionException, InterruptedException, IOException {
		Eventloop eventloop = Eventloop.create();
		final AsyncHttpClient httpClient = AsyncHttpClient.create(eventloop,
				AsyncDnsClient.create(eventloop).withDnsServerAddress(GOOGLE_PUBLIC_DNS));

		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				HttpResponse response = HttpResponse.ok200();
				response.addHeader(HttpHeaders.ALLOW, "GET,\r\n HEAD");
				callback.setResponse(response);
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenPort(PORT);
		server.listen();

		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.setResult(result.getHeader(HttpHeaders.ALLOW));
				httpClient.close();
				server.close(ignoreCompletionCallback());
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.setException(exception);
				httpClient.close();
				server.close(ignoreCompletionCallback());
			}
		});

		eventloop.run();
		assertEquals("GET,   HEAD", resultObserver.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}
}
