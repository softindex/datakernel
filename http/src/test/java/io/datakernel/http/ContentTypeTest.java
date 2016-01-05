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

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.http.HttpUtils.parseQ;
import static io.datakernel.util.ByteBufStrings.*;
import static java.nio.charset.Charset.forName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContentTypeTest {
	@Test
	public void testMediaType() {
		byte[] mediaType = encodeAscii("application/json");
		int hash = hashCodeLowerCaseAscii(mediaType);
		MediaType actual = MediaType.parse(mediaType, 0, mediaType.length, hash);
		assertTrue(MediaType.JSON == actual);
	}

	@Test
	public void testContentTypeParse() {
		byte[] contentType = encodeAscii("text/plain;param=value; url-form=www;CHARSET=UTF-8; a=v");
		ContentType actual = ContentType.parse(contentType, 0, contentType.length);
		assertTrue(MediaType.PLAIN_TEXT == actual.getMediaType());
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
		byte[] acceptCts = encodeAscii("text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
		List<AcceptContentType> list = AcceptContentType.parse(acceptCts, 0, acceptCts.length);
		assertEquals(5, list.size());
		assertEquals(80, list.get(4).getQ());
		assertTrue(MediaType.ANY == list.get(4).getMime());
	}

	@Test
	public void testRenderMime() {
		String expected = "application/json";
		ByteBuf buf = ByteBuf.allocate(expected.length());
		MediaType.JSON.render(buf);
		buf.flip();
		String actual = decodeAscii(buf);
		assertEquals(expected, actual);
	}

	@Test
	public void testRenderContentType() {
		String expected = "text/html; charset=utf-8";
		ByteBuf buf = ByteBuf.allocate(expected.length());
		ContentType type = ContentType.of(MediaType.HTML, HttpCharset.UTF_8);
		type.render(buf);
		buf.flip();
		String actual = decodeAscii(buf);
		assertEquals(expected, actual);
	}

	@Test
	public void testRenderAcceptContentType() {
		String expected = "text/html, application/xhtml+xml, application/xml; q=0.9, image/webp, */*; q=0.8";
		ByteBuf buf = ByteBuf.allocate(expected.length());
		List<AcceptContentType> acts = new ArrayList<>();
		acts.add(AcceptContentType.of(MediaType.HTML));
		acts.add(AcceptContentType.of(MediaType.XHTML_APP));
		acts.add(AcceptContentType.of(MediaType.XML_APP, 90));
		acts.add(AcceptContentType.of(MediaType.WEBP));
		acts.add(AcceptContentType.of(MediaType.ANY, 80));
		AcceptContentType.render(acts, buf);
		buf.flip();
		String actual = decodeAscii(buf);
		assertEquals(expected, actual);
	}
}