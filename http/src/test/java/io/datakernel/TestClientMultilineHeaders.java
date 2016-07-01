package io.datakernel;

import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.HttpUtils;

import java.util.concurrent.ExecutionException;

import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static io.datakernel.util.ByteBufStrings.decodeUTF8;

public class TestClientMultilineHeaders {
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Eventloop eventloop = new Eventloop();
		final AsyncHttpClient httpClient = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, 3_000L, HttpUtils.inetAddress("8.8.8.8")));

		final ResultCallbackFuture<String> resultObserver = new ResultCallbackFuture<>();

		httpClient.send(HttpRequest.get("http://www.bbc.com/sport/football/36408185"), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(final HttpResponse result) {
				try {
					resultObserver.onResult(decodeUTF8(result.getBody()));
				} catch (ParseException e) {
					onException(e);
				}
				httpClient.close();
			}

			@Override
			public void onException(Exception exception) {
				resultObserver.onException(exception);
				httpClient.close();
			}
		});

		eventloop.run();
		System.out.println(resultObserver.get());

	}
}
