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
import io.datakernel.async.Stages;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpRequest.post;
import static io.datakernel.http.HttpResponse.ok200;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.https.SslUtils.*;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static junit.framework.TestCase.assertEquals;

public class TestHttpsClientServer {
	private static final int PORT = 5590;
	private static final int SSL_PORT = 5591;

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
		//System.setProperty("javax.net.debug", "all");
	}

	private KeyManager[] keyManagers = createKeyManagers(new File("./src/test/resources/keystore.jks"), "testtest", "testtest");
	private TrustManager[] trustManagers = createTrustManagers(new File("./src/test/resources/truststore.jks"), "testtest");
	private AsyncServlet bobServlet = request -> Stages.of(ok200().withBody(wrapAscii("Hello, I am Bob!")));
	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private ExecutorService executor = newCachedThreadPool();
	private SSLContext context = createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom());

	public TestHttpsClientServer() throws Exception {
	}

	public static final InetAddress GOOGLE_PUBLIC_DNS = inetAddress("8.8.8.8");

	@Test
	public void testClientServerInteraction() throws Exception {
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, bobServlet)
				.withSslListenPort(createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom()), executor, SSL_PORT);

		final AsyncDnsClient dnsClient = AsyncDnsClient.create(eventloop)
				.withTimeout(500)
				.withDnsServerAddress(GOOGLE_PUBLIC_DNS);
		final AsyncHttpClient client = AsyncHttpClient.create(eventloop)
				.withConnectTimeout(500)
				.withDnsClient(dnsClient)
				.withSslEnabled(createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom()), executor);

		HttpRequest request = post("https://127.0.0.1:" + SSL_PORT).withBody(wrapAscii("Hello, I am Alice!"));

		server.listen();

		final CompletableFuture<String> future = client.send(request).thenApply(httpResponse -> {
			server.close();
			client.stop();
			return decodeAscii(httpResponse.getBody());
		}).toCompletableFuture();

		eventloop.run();
		executor.shutdown();

		assertEquals("Hello, I am Bob!", future.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testServesTwoPortsSimultaneously() throws Exception {
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, bobServlet)
				.withSslListenPort(context, executor, SSL_PORT)
				.withListenAddress(new InetSocketAddress("localhost", PORT));

		final AsyncDnsClient dnsClient = AsyncDnsClient.create(eventloop)
				.withTimeout(500)
				.withDnsServerAddress(GOOGLE_PUBLIC_DNS);

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient)
				.withConnectTimeout(500)
				.withSslEnabled(context, executor);

		final HttpRequest httpsRequest = post("https://127.0.0.1:" + SSL_PORT).withBody(wrapAscii("Hello, I am Alice!"));
		final HttpRequest httpRequest = post("http://127.0.0.1:" + PORT).withBody(wrapAscii("Hello, I am Alice!"));

		server.listen();

		final CompletableFuture<String> httpsFuture = client.send(httpsRequest)
				.thenApply(response -> decodeAscii(response.getBody()))
				.toCompletableFuture();

		final CompletableFuture<String> httpFuture = client.send(httpRequest)
				.thenApply(response -> decodeAscii(response.getBody()))
				.toCompletableFuture();

		httpFuture.runAfterBoth(httpsFuture, () -> {
			server.close();
			client.stop();
		});

		eventloop.run();
		executor.shutdown();

		assertEquals(httpFuture.get(), httpsFuture.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}