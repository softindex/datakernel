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

package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static io.datakernel.bytebuf.ByteBufPool.getCreatedItems;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItems;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.http.HttpHeaders.of;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class HttpMessageTest {
	public void assertHttpResponseEquals(String expected, HttpResponse result) {
		ByteBuf buf = result.toByteBuf();
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
		buf.recycle();
		result.recycleBufs();
	}

	public void assertHttpRequestEquals(String expected, HttpRequest request) {
		ByteBuf buf = request.toByteBuf();
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
		buf.recycle();
		request.recycleBufs();
	}

	@Test
	public void testHttpResponse() {
		assertHttpResponseEquals("HTTP/1.1 100 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(100));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(200));
		assertHttpResponseEquals("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(400));
		assertHttpResponseEquals("HTTP/1.1 405 Error\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(405));
		assertHttpResponseEquals("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n", HttpResponse.ofCode(500));
		assertHttpResponseEquals("HTTP/1.1 502 Error\r\nContent-Length: 9\r\n\r\n" +
				"Error 502", HttpResponse.ofCode(502).withBody("Error 502".getBytes(StandardCharsets.UTF_8)));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=value1\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.ofCode(200).withCookies(Collections.singletonList(HttpCookie.of("cookie1", "value1"))));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=value1, cookie2=value2\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.ofCode(200).withCookies(asList(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=value1, cookie2=value2\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.ofCode(200).withCookies(asList(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testHttpRequest() {
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n",
				HttpRequest.get("http://test.com/index.html"));
		assertHttpRequestEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n",
				HttpRequest.post("http://test.com/index.html"));
		assertHttpRequestEquals("CONNECT /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n",
				HttpRequest.of(HttpMethod.CONNECT, "http://test.com/index.html"));
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=value1\r\n\r\n",
				HttpRequest.get("http://test.com/index.html").withCookie(HttpCookie.of("cookie1", "value1")));
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=value1; cookie2=value2\r\n\r\n",
				HttpRequest.get("http://test.com/index.html").withCookies(asList(HttpCookie.of("cookie1", "value1"), HttpCookie.of("cookie2", "value2"))));

		HttpRequest request = HttpRequest.post("http://test.com/index.html");
		ByteBuf buf = ByteBufPool.allocate(100);
		buf.put("/abc".getBytes(), 0, 4);
		request.setBody(buf);
		assertHttpRequestEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 4\r\n\r\n/abc", request);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private static String getHeaderValue(HttpMessage message, HttpHeader header) {
		return message.getHeader(header);
	}

	@Test
	public void testMultiHeaders() {
		HttpResponse response = HttpResponse.ofCode(200);
		HttpHeader header1 = of("header1");
		HttpHeader HEADER1 = of("HEADER1");
		HttpHeader header2 = of("header2");

		assertTrue(response.headers.isEmpty());
		assertNull(getHeaderValue(response, header1));
		assertNull(getHeaderValue(response, header2));

		response.addHeader(header1, "value1");
		response.addHeader(header2, "value2");
		response.addHeader(HEADER1, "VALUE1");

		assertEquals(3, response.headers.size());

		assertEquals("value1", response.getHeader(header1));
		assertEquals("value1", response.getHeader(HEADER1));
		assertEquals("value2", response.getHeader(header2));

		assertEquals("value1", response.getHeaders().get(header1));
		assertEquals("value1", response.getHeaders().get(HEADER1));
		assertEquals("value2", response.getHeaders().get(header2));

		assertEquals(asList("value1", "VALUE1"), response.getAllHeaders().get(header1));
		assertEquals(asList("value1", "VALUE1"), response.getAllHeaders().get(HEADER1));
		assertEquals(asList("value2"), response.getAllHeaders().get(header2));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}