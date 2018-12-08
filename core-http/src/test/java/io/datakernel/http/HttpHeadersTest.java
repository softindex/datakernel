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

import io.datakernel.exception.ParseException;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import static io.datakernel.http.ContentTypes.JSON_UTF_8;
import static io.datakernel.http.HttpHeaderValue.*;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.MediaTypes.ANY_IMAGE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class HttpHeadersTest {

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testValuesToStrings() throws ParseException {
		HttpRequest request = HttpRequest.post("http://example.com")
				.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
				.withHeader(ACCEPT, ofAcceptMediaTypes(AcceptMediaType.of(ANY_IMAGE, 50), AcceptMediaType.of(MediaTypes.HTML)))
				.withHeader(ACCEPT_CHARSET, ofAcceptCharsets(AcceptCharset.of(UTF_8), AcceptCharset.of(ISO_8859_1)))
				.withCookies(HttpCookie.of("key1", "value1"), HttpCookie.of("key2"), HttpCookie.of("key3", "value2"));

		assertEquals("application/json; charset=utf-8", request.getHeader(CONTENT_TYPE));
		assertEquals("image/*; q=0.5, text/html", request.getHeader(ACCEPT));
		assertEquals("utf-8, iso-8859-1", request.getHeader(ACCEPT_CHARSET));
		assertEquals("key1=value1; key2; key3=value2", request.getHeader(COOKIE));

		HttpResponse response = HttpResponse.ofCode(200)
				.withHeader(DATE, HttpHeaderValue.ofTimestamp(1486944000021L));

		assertEquals("Mon, 13 Feb 2017 00:00:00 GMT", response.getHeader(DATE));
	}
}
