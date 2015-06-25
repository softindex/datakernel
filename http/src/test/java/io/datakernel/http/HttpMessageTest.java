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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import java.net.HttpCookie;
import java.util.List;

import static io.datakernel.http.HttpHeader.headerOfString;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class HttpMessageTest {
	private final ByteBufPool pool = ByteBufPool.defaultInstance();

	public void assertHttpResponseEquals(String expected, HttpResponse result) {
		ByteBuf buf = result.write(pool);
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
		buf.recycle();
	}

	public void assertHttpRequestEquals(String expected, HttpRequest request) {
		ByteBuf buf = request.write(pool);
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
		buf.recycle();
	}

	@Test
	public void testHttpResponse() {
		assertHttpResponseEquals("HTTP/1.1 100 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.create(100));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", HttpResponse.create(200));
		assertHttpResponseEquals("HTTP/1.1 400 Bad Request\r\nContent-Length: 77\r\n\r\n" +
				"Your browser (or proxy) sent a request that this server could not understand.", HttpResponse.create(400));
		assertHttpResponseEquals("HTTP/1.1 405 Error\r\nContent-Length: 0\r\n\r\n", HttpResponse.create(405));
		assertHttpResponseEquals("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 81\r\n\r\n" +
				"The server encountered an internal error and was unable to complete your request.", HttpResponse.create(500));
		assertHttpResponseEquals("HTTP/1.1 502 Error\r\nContent-Length: 9\r\n\r\n" +
				"Error 502", HttpResponse.create(502).body("Error 502".getBytes(Charsets.UTF_8)));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie1=value1;Path=/\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.create(200).cookie(new HttpCookie("cookie1", "value1")));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie2=value2;Path=/\r\nSet-Cookie: cookie1=value1;Path=/\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.create(200).cookie(new HttpCookie("cookie1", "value1")).cookie(new HttpCookie("cookie2", "value2")));
		assertHttpResponseEquals("HTTP/1.1 200 OK\r\nSet-Cookie: cookie2=value2;Path=/\r\nSet-Cookie: cookie1=value1;Path=/\r\nContent-Length: 0\r\n\r\n",
				HttpResponse.create(200).cookie(asList(new HttpCookie("cookie1", "value1"), new HttpCookie("cookie2", "value2"))));
	}

	@Test
	public void testHttpRequest() {
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\n\r\n",
				HttpRequest.get("http://test.com/index.html"));
		assertHttpRequestEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n",
				HttpRequest.post("http://test.com/index.html"));
		assertHttpRequestEquals("CONNECT /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 0\r\n\r\n",
				HttpRequest.create(HttpMethod.CONNECT).url("http://test.com/index.html"));
		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=\"value1\"\r\n\r\n",
				HttpRequest.get("http://test.com/index.html").cookie(new HttpCookie("cookie1", "value1")));
//		assertHttpRequestEquals("GET /index.html HTTP/1.1\r\nHost: test.com\r\nCookie: cookie1=\"value1\"; cookie2=\"value2\"\r\n\r\n",
//				HttpRequest.get("http://test.com/index.html").cookie(asList(new HttpCookie("cookie1", "value1"), new HttpCookie("cookie2", "value2"))));

		HttpRequest request = HttpRequest.post("http://test.com/index.html");
		ByteBuf buf = pool.allocate(100);
		buf.put("/abc".getBytes(), 0, 4);
		buf.flip();
		request.setBody(buf);
		assertHttpRequestEquals("POST /index.html HTTP/1.1\r\nHost: test.com\r\nContent-Length: 4\r\n\r\n/abc", request);
		buf.recycle();
	}

	private static String getHeaderValue(HttpMessage message, String headerName) {
		return message.getHeaderString(headerOfString(headerName));
	}

	private static List<String> getHeaderValues(HttpMessage message, String headerName) {
		List<String> result = Lists.newArrayList();
		HttpHeaderValue value = message.getHeaderValue(headerOfString(headerName));
		if (value == null)
			return result;
		result.add(value.toString());
		while (value.next() != null) {
			value = value.next();
			result.add(value.toString());
		}
		return result;
	}

	@Test
	public void testMultiHeaders() {
		HttpResponse h = HttpResponse.create(200);
		assertTrue(h.getHeaders().isEmpty());
		assertNull(getHeaderValue(h, "h1"));
		assertNull(getHeaderValue(h, "h2"));

		h.header(headerOfString("h1"), "v1");
		h.header(headerOfString("h2"), "v2");
		h.header(headerOfString("h1"), "v3");

		assertEquals(2, h.getHeaders().size());
		assertEquals(asList("v3", "v1"), getHeaderValues(h, "h1"));
		assertEquals(singletonList("v2"), getHeaderValues(h, "h2"));
		assertEquals("v2", getHeaderValue(h, "h2"));
		assertEquals("v3", getHeaderValue(h, "h1"));

		h.header(headerOfString("h2"), "v4");
		assertEquals(asList("v3", "v1"), getHeaderValues(h, "h1"));
		assertEquals(asList("v4", "v2"), getHeaderValues(h, "h2"));
		assertEquals("v4", getHeaderValue(h, "h2"));
		assertEquals("v3", getHeaderValue(h, "h1"));

		h.headerReset(headerOfString("h1"), "v5");
		assertEquals(2, h.getHeaders().size());
		assertEquals(singletonList("v5"), getHeaderValues(h, "h1"));
		assertEquals(asList("v4", "v2"), getHeaderValues(h, "h2"));
		assertEquals("v4", getHeaderValue(h, "h2"));
		assertEquals("v5", getHeaderValue(h, "h1"));
	}
}
