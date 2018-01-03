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

package io.datakernel;

import io.datakernel.async.Stages;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.http.MediaTypes.*;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;

public class ClientStressTest {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String PATH_TO_URLS = "./src/test/resources/urls.txt";

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private ExecutorService executor = newCachedThreadPool();
	private Random random = new Random();
	private Iterator<String> urls = getUrls().iterator();

	private AsyncServlet servlet = request -> {
		test();
		return Stages.of(HttpResponse.ok200());
	};
	private AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenAddress(new InetSocketAddress("localhost", 1234));

	private final SSLContext context = SSLContext.getDefault();

	private AsyncHttpClient client = AsyncHttpClient.create(eventloop)
			.withDnsClient(AsyncDnsClient.create(eventloop).withDnsServerAddress(inetAddress("8.8.8.8")))
			.withSslEnabled(context, executor);

	private ClientStressTest() throws Exception {}

	private void doTest() throws IOException {
		server.listen();
		eventloop.run();
	}

	private void test() {
		int delay = random.nextInt(10000);
		final String url = urls.next();
		if (url != null) {
			eventloop.schedule(eventloop.currentTimeMillis() + delay, () -> {
				logger.info("sending request to: {}", url);
				client.send(formRequest(url, random.nextBoolean())).whenComplete((response, throwable) -> {
					if (throwable != null) {
						logger.error("url: {}, failed", url, throwable);
					} else {
						logger.info("url: {}, succeed", url);
					}
				});
				test();
			});
		} else {
			server.close();
		}
	}

	private static final Pattern separator = Pattern.compile("\n");

	private List<String> getUrls() throws IOException {
		return separator.splitAsStream(new String(Files.readAllBytes(Paths.get(PATH_TO_URLS))))
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(toList());
	}

	private HttpRequest formRequest(String url, boolean keepAlive) {
		HttpRequest request = HttpRequest.get(url);
		request.addHeader(CACHE_CONTROL, "max-age=0");
		request.addHeader(ACCEPT_ENCODING, "gzip, deflate, sdch");
		request.addHeader(ACCEPT_LANGUAGE, "en-US,en;q=0.8");
		request.addHeader(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36");
		request.setAccept(AcceptMediaType.of(HTML),
				AcceptMediaType.of(XHTML_APP),
				AcceptMediaType.of(XML_APP, 90),
				AcceptMediaType.of(WEBP),
				AcceptMediaType.of(ANY, 80));
		if (keepAlive) {
			request.addHeader(CONNECTION, "keep-alive");
		}
		return request;
	}

	public static void main(String[] args) throws Exception {
		new ClientStressTest().doTest();
	}
}
