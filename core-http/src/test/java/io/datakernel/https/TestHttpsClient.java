/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.CachedAsyncDnsClient;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AcceptMediaType;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.Executors;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.http.HttpHeaderValue.ofAcceptMediaTypes;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.http.MediaTypes.*;
import static org.junit.Assert.assertEquals;

public final class TestHttpsClient {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testClient() throws NoSuchAlgorithmException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		AsyncDnsClient dnsClient = CachedAsyncDnsClient.create(eventloop, RemoteAsyncDnsClient.create(eventloop)
				.withTimeout(Duration.ofMillis(500))
				.withDnsServerAddress(inetAddress("8.8.8.8")));

		AsyncHttpClient client = AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient)
				.withSslEnabled(SSLContext.getDefault(), Executors.newSingleThreadExecutor());
		Integer code = await(client.request(HttpRequest.get("https://en.wikipedia.org/wiki/Wikipedia")
				.withHeader(CACHE_CONTROL, "max-age=0")
				.withHeader(ACCEPT_ENCODING, "gzip, deflate, sdch")
				.withHeader(ACCEPT_LANGUAGE, "en-US,en;q=0.8")
				.withHeader(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36")
				.withHeader(ACCEPT, ofAcceptMediaTypes(
						AcceptMediaType.of(HTML),
						AcceptMediaType.of(XHTML_APP),
						AcceptMediaType.of(XML_APP, 90),
						AcceptMediaType.of(WEBP),
						AcceptMediaType.of(ANY, 80))))
				.map(HttpResponse::getCode));

		assertEquals((Integer) 200, code);
	}
}
