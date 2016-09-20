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
import io.datakernel.exception.ParseException;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static java.nio.charset.Charset.forName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpCharsetTest {
	@Test
	public void testCharsetRender() {
		String expected = "utf-8";
		HttpCharset charset = HttpCharset.UTF_8;
		byte[] container = new byte[5];
		int pos = HttpCharset.render(charset, container, 0);
		assertEquals(5, pos);
		assertEquals(expected, new String(container));
	}

	@Test
	public void testParser() {
		HttpCharset expected = HttpCharset.US_ASCII;
		byte[] asciis = "us-ascii".getBytes(forName("ASCII"));
		HttpCharset actual = HttpCharset.parse(asciis, 0, asciis.length);
		assertTrue(expected == actual);
	}

	@Test
	public void testConverters() {
		HttpCharset expected = HttpCharset.US_ASCII;
		Charset charset = expected.toJavaCharset();
		HttpCharset actual = HttpCharset.of(charset);
		assertTrue(expected == actual);
	}

	@Test
	public void testAcceptCharset() throws ParseException {
		byte[] bytes = encodeAscii("iso-8859-5, unicode-1-1;q=0.8");
		List<AcceptCharset> charsets = AcceptCharset.parse(bytes, 0, bytes.length);
		assertEquals(2, charsets.size());
		assertTrue(forName("ISO-8859-5") == charsets.get(0).getCharset());
		assertEquals(80, charsets.get(1).getQ());
	}

	@Test
	public void testRenderAcceptCharset() {
		String expected = "iso-8859-1, UTF-16; q=0.8";
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[expected.length()]);
		List<AcceptCharset> chs = new ArrayList<>();
		chs.add(AcceptCharset.of(StandardCharsets.ISO_8859_1));
		chs.add(AcceptCharset.of(StandardCharsets.UTF_16, 80));
		AcceptCharset.render(chs, buf);
		String actual = decodeAscii(buf);
		assertEquals(expected, actual);
	}
}
