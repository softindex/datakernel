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
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AcceptMediaType;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.http.MediaTypes.*;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static junit.framework.TestCase.assertEquals;

public class TestHttpsClient {
	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
		//System.setProperty("javax.net.debug", "all");
	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Ignore("requires internet connection")
	@Test
	public void testClient() throws NoSuchAlgorithmException, ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = newCachedThreadPool();

		AsyncDnsClient dnsClient = AsyncDnsClient.create(eventloop)
				.withTimeout(500)
				.withDnsServerAddress(inetAddress("8.8.8.8"));

		AsyncHttpClient client = AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient)
				.withSslEnabled(SSLContext.getDefault(), executor);

		String url = "https://en.wikipedia.org/wiki/Wikipedia";
		CompletableFuture<Integer> future = client.send(get(url)).thenApply(response -> {
			client.stop();
			return response.getCode();
		}).toCompletableFuture();

		eventloop.run();
		executor.shutdown();

		assertEquals(200, (int) future.get());
	}

	private HttpRequest get(String url) {
		return HttpRequest.get(url)
				.withHeader(CONNECTION, "keep-alive")
				.withHeader(CACHE_CONTROL, "max-age=0")
				.withHeader(ACCEPT_ENCODING, "gzip, deflate, sdch")
				.withHeader(ACCEPT_LANGUAGE, "en-US,en;q=0.8")
				.withHeader(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36")
				.withAccept(AcceptMediaType.of(HTML),
						AcceptMediaType.of(XHTML_APP),
						AcceptMediaType.of(XML_APP, 90),
						AcceptMediaType.of(WEBP),
						AcceptMediaType.of(ANY, 80));
	}
}
