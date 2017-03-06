package io.datakernel.http;

import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.IAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufPool.getCreatedItems;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItems;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AbstractHttpConnectionTest {
	private static final int PORT = 5050;
	private InetAddress GOOGLE_PUBLIC_DNS = HttpUtils.inetAddress("8.8.8.8");

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private IAsyncDnsClient dnsClient = AsyncDnsClient.create(eventloop).withTimeout(300).withDnsServerAddress(GOOGLE_PUBLIC_DNS);

	private AsyncServlet servlet = new AsyncServlet() {
		@Override
		public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
			callback.setResult(createMultiLineHeaderWithInitialBodySpacesResponse());
		}
	};

	private AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenPort(PORT);
	private AsyncHttpClient client = AsyncHttpClient.create(eventloop).withDnsClient(dnsClient);

	@Test
	public void testMultiLineHeader() throws IOException {
		server.listen();
		final Map<String, String> data = new HashMap<>();
		client.send(HttpRequest.get("http://127.0.0.1:" + PORT), new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				data.put("body", decodeAscii(result.getBody()));
				data.put("header", result.getHeader(HttpHeaders.CONTENT_TYPE));
				client.stop(IgnoreCompletionCallback.create());
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				e.printStackTrace();
				fail();
			}
		});

		eventloop.run();
		assertEquals("text/           html", data.get("header"));
		assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", data.get("body"));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private HttpResponse createMultiLineHeaderWithInitialBodySpacesResponse() {
		HttpResponse response = HttpResponse.ok200();
		response.addHeader(HttpHeaders.DATE, "Mon, 27 Jul 2009 12:28:53 GMT");
		response.addHeader(HttpHeaders.CONTENT_TYPE, "text/\n          html");
		response.setBody(ByteBufStrings.wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>"));
		return response;
	}
}