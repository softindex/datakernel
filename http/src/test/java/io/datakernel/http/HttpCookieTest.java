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
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.exception.ParseException;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class HttpCookieTest {

	@Test
	public void testParser() throws ParseException {
		String cookieString = "name1=\"value1\"; expires=Thu, 01 Jan 2015 00:00:00 GMT; Secure; name2=value2; HttpOnly";
		List<HttpCookie> httpCookies = new ArrayList<>();
		HttpCookie.parse(cookieString, httpCookies);
		assertEquals(2, httpCookies.size());
		HttpCookie cookie1 = httpCookies.get(0);
		HttpCookie cookie2 = httpCookies.get(1);

		assertTrue(cookie1.getName().equals("name1"));
		assertTrue(cookie1.getValue().equals("value1"));
		GregorianCalendar calendar = new GregorianCalendar(2015, 0, 1);
		calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
		assertEquals(cookie1.getExpirationDate(), calendar.getTime());
		assertEquals(0, cookie1.getMaxAge());
		assertTrue(cookie1.isSecure());
		assertFalse(cookie1.isHttpOnly());

		assertTrue(cookie2.getName().equals("name2"));
		assertTrue(cookie2.getValue().equals("value2"));
		assertFalse(cookie2.isSecure());
		assertTrue(cookie2.isHttpOnly());
	}

	@Test
	public void testRender() {
		Date date = new Date(987654321098l); // "Thu, 19 Apr 2001 04:25:21 GMT";
		HttpCookie cookie = HttpCookie.of("name", "value")
				.withExpirationDate(date)
				.withMaxAge(10)
				.withPath("/test")
				.withDomain("www.google.com")
				.withSecure(true)
				.withHttpOnly(true)
				.withExtension("Alhambra site");

		String expected = "name=\"value\"; Expires=Thu, 19 Apr 2001 04:25:21 GMT; Max-Age=10; Domain=www.google.com; " +
				"Path=/test; Secure; HttpOnly; \"Alhambra site\"";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		cookie.renderFull(buf);
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
	}

	@Test
	public void testRenderMany() {
		Date date = new Date(987654321098l); // "Thu, 19 Apr 2001 04:25:21 GMT";
		HttpCookie cookie1 = HttpCookie.of("name1", "value1")
				.withExpirationDate(date)
				.withMaxAge(10)
				.withPath("/test")
				.withDomain("www.google.com")
				.withSecure(true);

		HttpCookie cookie2 = HttpCookie.of("name2", "value2")
				.withHttpOnly(true)
				.withExtension("Alhambra site");
		HttpCookie cookie3 = HttpCookie.of("name3");

		String expected = "name1=\"value1\"; name2=\"value2\"; name3";

		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		HttpCookie.renderSimple(Arrays.asList(cookie1, cookie2, cookie3), buf);
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
	}
}
