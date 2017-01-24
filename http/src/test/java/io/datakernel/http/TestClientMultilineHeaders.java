package io.datakernel.http;

import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
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

		final ResultCallbackFuture<String> resultObserver = ResultCallbackFuture.create();

		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse response = HttpResponse.ok200();
				response.addHeader(HttpHeaders.ALLOW, "GET,\r\n HEAD");
				callback.setResult(response);
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenPort(PORT);
		server.listen();

		httpClient.send(HttpRequest.get("http://127.0.0.1:" + PORT), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.setResult(result.getHeader(HttpHeaders.ALLOW));
				httpClient.close();
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.setException(exception);
				httpClient.close();
				server.close(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		assertEquals("GET,   HEAD", resultObserver.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
