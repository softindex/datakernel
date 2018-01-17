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

import org.junit.Test;

import java.util.Date;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.http.ContentTypes.JSON_UTF_8;
import static io.datakernel.http.MediaTypes.ANY_IMAGE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class HttpHeadersTest {
	@Test
	public void testValuesToStrings() {
		HttpRequest request = HttpRequest.post("http://example.com")
				.withContentType(JSON_UTF_8)
				.withAccept(AcceptMediaType.of(ANY_IMAGE, 50), AcceptMediaType.of(MediaTypes.HTML))
				.withAcceptCharsets(AcceptCharset.of(UTF_8), AcceptCharset.of(ISO_8859_1))
				.withCookies(HttpCookie.of("key1", "value1"), HttpCookie.of("key2"), HttpCookie.of("key3", "value2"));

		Map<HttpHeader, String> headers = request.getHeaders();
		assertEquals("application/json; charset=utf-8", headers.get(HttpHeaders.CONTENT_TYPE));
		assertEquals("image/*; q=0.5, text/html", headers.get(HttpHeaders.ACCEPT));
		assertEquals("utf-8, iso-8859-1", headers.get(HttpHeaders.ACCEPT_CHARSET));
		assertEquals("key1=value1; key2; key3=value2", headers.get(HttpHeaders.COOKIE));

		HttpResponse response = HttpResponse.ofCode(200)
				.withDate(new Date(1486944000021L));

		headers = response.getHeaders();
		assertEquals("Mon, 13 Feb 2017 00:00:00 GMT", headers.get(HttpHeaders.DATE));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}