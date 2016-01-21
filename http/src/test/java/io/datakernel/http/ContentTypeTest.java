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
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Utils.parseQ;
import static io.datakernel.http.MediaTypes.*;
import static io.datakernel.util.ByteBufStrings.*;
import static java.nio.charset.Charset.forName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContentTypeTest {
	@Test
	public void testMediaType() {
		byte[] mediaType = encodeAscii("application/json");
		int hash = hashCodeLowerCaseAscii(mediaType);
		MediaType actual = MediaTypes.parse(mediaType, 0, mediaType.length, hash);
		assertTrue(JSON == actual);
	}

	@Test
	public void testContentTypeParse() {
		byte[] contentType = encodeAscii("text/plain;param=value; url-form=www;CHARSET=UTF-8; a=v");
		ContentType actual = ContentType.parse(contentType, 0, contentType.length);
		assertTrue(MediaTypes.PLAIN_TEXT == actual.getMediaType());
		assertTrue(forName("UTF-8") == actual.getCharset());
	}

	@Test
	public void testQParser() {
		byte[] num = encodeAscii("0.12313");
		int q = parseQ(num, 0, num.length);
		assertEquals(12, q);

		num = encodeAscii("1.0");
		q = parseQ(num, 0, num.length);
		assertEquals(100, q);
	}

	@Test
	public void testAcceptContentType() {
		byte[] acceptCts = encodeAscii("text/html;q=0.1, " +
				"application/xhtml+xml; method=get; q=0.3; bool=true," +
				"application/xml;q=0.9," +
				"image/webp," +
				"*/*;q=0.8," +
				"unknown/mime");
		List<AcceptMediaType> actual = AcceptMediaType.parse(acceptCts, 0, acceptCts.length);
		List<AcceptMediaType> expected = new ArrayList<>();
		expected.add(AcceptMediaType.of(HTML, 10));
		expected.add(AcceptMediaType.of(XHTML_APP, 30));
		expected.add(AcceptMediaType.of(XML_APP, 90));
		expected.add(AcceptMediaType.of(WEBP));
		expected.add(AcceptMediaType.of(ANY, 80));
		expected.add(AcceptMediaType.of(MediaType.of("unknown/mime")));
		assertEquals(expected.toString(), actual.toString());
	}

	@Test
	public void testRenderMime() {
		String expected = "application/json";
		ByteBuf buf = ByteBuf.allocate(expected.length());
		MediaTypes.render(JSON, buf);
		buf.flip();
		String actual = decodeAscii(buf);
		assertEquals(expected, actual);
	}

	@Test
	public void testRenderContentType() {
		String expected = "text/html; charset=utf-8";
		ByteBuf buf = ByteBuf.allocate(expected.length());
		ContentType type = ContentType.of(HTML, StandardCharsets.UTF_8);
		ContentType.render(type, buf);
		buf.flip();
		String actual = decodeAscii(buf);
		assertEquals(expected, actual);
	}

	@Test
	public void testRenderAcceptContentType() {
		String expected = "text/html, application/xhtml+xml, application/xml; q=0.9, image/webp, */*; q=0.8";
		ByteBuf buf = ByteBuf.allocate(expected.length());
		List<AcceptMediaType> acts = new ArrayList<>();
		acts.add(AcceptMediaType.of(HTML));
		acts.add(AcceptMediaType.of(XHTML_APP));
		acts.add(AcceptMediaType.of(MediaTypes.XML_APP, 90));
		acts.add(AcceptMediaType.of(WEBP));
		acts.add(AcceptMediaType.of(MediaTypes.ANY, 80));
		AcceptMediaType.render(acts, buf);
		buf.flip();
		String actual = decodeAscii(buf);
		assertEquals(expected, actual);
	}
}