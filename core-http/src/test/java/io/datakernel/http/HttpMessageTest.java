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

package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;

import static io.datakernel.http.HttpHeaders.of;
import static io.datakernel.http.HttpMethod.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class HttpMessageTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static void assertHttpMessageEquals(String expected, HttpMessage message) {
		ByteBuf buf = AbstractHttpConnection.renderHttpMessage(message);
		assertNotNull(buf);
		String actual = ByteBufStrings.asAscii(buf);

		assertEquals(new LinkedHashSet<>(asList(expected.split("\r\n"))), new LinkedHashSet<>(asList(actual.split("\r\n"))));
		message.recycle();
	}

	@Test
	public void testHttpResponse() {
		assertHttpMessageEquals("HTTP/1.1 100 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(100));
		assertHttpMessageEquals("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(200));
		assertHttpMessageEquals("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(400));
		assertHttpMessageEquals("HTTP/1.1 405 Error\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(405));
		assertHttpMessageEquals("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(500));
		assertHttpMessageEquals("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\n" +
				"Bad Gateway", HttpResponse.ofCode(502).withBody("Bad Gateway".getBytes(StandardCharsets.UTF_8)));
		assertHttpMessageEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=value1\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.ofCode(200).withCookies(Collections.singletonList(HttpCookie.of("cookie1", "value1"))));
		assertHttpMessageEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=value1\r\nSet-Cookie: cookie2=value2\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.ofCode(200).withCookies(asList(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));
		assertHttpMessageEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=value1\r\nSet-Cookie: cookie2=value2\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.ofCode(200).withCookies(asList(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));
	}

	@Test
	public void testHttpRequest() {
		assertHttpMessageEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n", HttpRequest.get("http://test.com/index.html"));
		assertHttpMessageEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n", HttpRequest.post("http://test.com/index.html"));
		assertHttpMessageEquals("CONNECT /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n", HttpRequest.of(HttpMethod.CONNECT, "http://test.com/index.html"));
		assertHttpMessageEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=value1\r\n\r\n", HttpRequest.get("http://test.com/index.html").withCookie(HttpCookie.of("cookie1", "value1")));
		assertHttpMessageEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=value1; cookie2=value2\r\n\r\n", HttpRequest.get("http://test.com/index.html").withCookies(asList(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));

		HttpRequest request = HttpRequest.post("http://test.com/index.html");
		ByteBuf buf = ByteBufPool.allocate(100);
		buf.put("/abc".getBytes(), 0, 4);
		request.setBody(buf);
		assertHttpMessageEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 4\r\n\r\n/abc", request);
	}

	@Test
	public void testHttpRequestWithNoPayload() {
		assertHttpMessageEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n", HttpRequest.of(GET, "http://test.com/index.html"));
		assertHttpMessageEquals("HEAD /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n", HttpRequest.of(HEAD, "http://test.com/index.html"));
		assertHttpMessageEquals("CONNECT /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n", HttpRequest.of(CONNECT, "http://test.com/index.html"));
		assertHttpMessageEquals("OPTIONS /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n", HttpRequest.of(OPTIONS, "http://test.com/index.html"));
		assertHttpMessageEquals("TRACE /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n", HttpRequest.of(TRACE, "http://test.com/index.html"));

		assertHttpMessageEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n", HttpRequest.of(POST, "http://test.com/index.html"));
		assertHttpMessageEquals("PUT /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n", HttpRequest.of(PUT, "http://test.com/index.html"));
		assertHttpMessageEquals("DELETE /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n", HttpRequest.of(DELETE, "http://test.com/index.html"));
		assertHttpMessageEquals("PATCH /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n", HttpRequest.of(PATCH, "http://test.com/index.html"));
	}

	@Test
	public void testMultiHeaders() {
		HttpResponse response = HttpResponse.ofCode(200);
		HttpHeader header1 = of("header1");
		HttpHeader HEADER1 = of("HEADER1");

		response.addHeader(header1, "value1");
		response.addHeader(HEADER1, "VALUE1");
		assertHttpMessageEquals("HTTP/1.1 200 OK\r\nheader1: value1\r\nHEADER1: VALUE1\r\nContent-Length: 0\r\n\r\n", response);
	}
}
