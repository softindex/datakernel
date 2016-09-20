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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncHttpServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.https.SslUtils.*;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestHttpsServer {
	private static final int PORT = 5568;

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
//		System.setProperty("javax.net.debug", "all");
	}

	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create();
		ExecutorService executor = newCachedThreadPool();

		AsyncHttpServlet bobServlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				callback.onResult(HttpResponse.ok200().withBody(wrapAscii("Hello, I am Bob!")));
			}
		};

		KeyManager[] keyManagers = createKeyManagers(new File("./src/test/resources/keystore.jks"), "testtest", "testtest");
		TrustManager[] trustManagers = createTrustManagers(new File("./src/test/resources/truststore.jks"), "testtest");

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, bobServlet)
				.withSslListenPort(createSslContext("TLSv1", keyManagers, trustManagers, new SecureRandom()), executor, PORT)
				.withListenPort(5569);

		System.out.println("https://127.0.0.1:" + PORT);

		server.listen();
		eventloop.run();
	}
}
