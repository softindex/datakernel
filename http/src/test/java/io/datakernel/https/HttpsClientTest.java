/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.https;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AcceptMediaType;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.http.MediaTypes.*;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static junit.framework.TestCase.assertEquals;

public class HttpsClientTest {
	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
		//System.setProperty("javax.net.debug", "all");
	}

	@Test
	public void testClient() throws NoSuchAlgorithmException, ExecutionException, InterruptedException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		final AsyncHttpClient client = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, defaultDatagramSocketSettings(), 500, inetAddress("8.8.8.8")))
				.enableSsl(SSLContext.getDefault(), executor);

		final ResultCallbackFuture<Integer> callback = new ResultCallbackFuture<>();

		String url = "https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLEngine.html";
		client.send(get(url), 500000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				callback.onResult(result.getCode());
				client.close();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
				client.close();
			}
		});

		eventloop.run();
		executor.shutdown();

		assertEquals(200, (int) callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private HttpRequest get(String url) {
		return HttpRequest.get(url)
				.header(CONNECTION, "keep-alive")
				.header(CACHE_CONTROL, "max-age=0")
				.header(ACCEPT_ENCODING, "gzip, deflate, sdch")
				.header(ACCEPT_LANGUAGE, "en-US,en;q=0.8")
				.header(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36")
				.accept(AcceptMediaType.of(HTML),
						AcceptMediaType.of(XHTML_APP),
						AcceptMediaType.of(XML_APP, 90),
						AcceptMediaType.of(WEBP),
						AcceptMediaType.of(ANY, 80));
	}
}
