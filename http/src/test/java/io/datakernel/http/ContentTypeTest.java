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
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.http.ContentType.*;
import static org.junit.Assert.assertEquals;

public class ContentTypeTest {
	@Test
	public void testParse() {
		String string = "text/plain, image/bmp,application/ogg";
		List<ContentType> expected = Arrays.asList(PLAIN_TEXT, BMP, OGG_APP);
		List<ContentType> actual = new ArrayList<>();
		parse(ByteBufStrings.encodeAscii(string), 0, string.length(), actual);
		assertEquals(expected, actual);
	}

	@Test
	public void testCaseInsensitive() {
		ContentType ct1 = getByName("APPLICATION/JSON");
		ContentType ct2 = JSON;
		assertEquals(ct1, ct2);

		ct1 = getByName("TEXT/PLAIN");
		ct2 = getByName("text/plain"); // ContentType.PLAIN_TEXT
		assertEquals(ct1, ct2);

		ct1 = getByName("*/*");
		ct2 = ANY;
		assertEquals(ct1, ct2);
	}

	@Test
	public void testRender() {
		String expected = "text/plain,image/bmp,application/ogg";
		List<ContentType> types = Arrays.asList(PLAIN_TEXT, BMP, OGG_APP);
		ByteBuf buf = ByteBuf.allocate(expected.length());
		render(types, buf);
		assertEquals(expected, ByteBufStrings.decodeAscii(buf.array()));
	}

	@Test
	public void testGetExt() {
		ContentType expected = HTML;
		ContentType actual = getByExt("html");
		assertEquals(expected, actual);
	}

	@Test
	public void testRenderWithParameters() {
		List<ContentType> actual = Arrays.asList(PLAIN_TEXT, ANY_TEXT.specify(0.7, Charset.forName("UTF-8")));
		String expected = "text/plain,text/*;q=0.7;charset=utf-8";
		ByteBuf buf = ByteBuf.allocate(expected.length());
		render(actual, buf);
		buf.flip();
		assertEquals(expected, ByteBufStrings.decodeAscii(buf));
	}

	@Test
	public void testParseWithParameters() {
		List<ContentType> expected = Arrays.asList(PLAIN_TEXT, MP3,
				ANY_TEXT.specify(0.7, Charset.forName("UTF-8")),
				OGG_VIDEO.specify(0.7, Charset.forName("ISO-8859-1")));
		String string = "text/plain,audio/mp3,text/*; q=0.7; charset=utf-8,video/ogg; q=0.8";
		ByteBuf buf = ByteBufStrings.wrapAscii(string);
		List<ContentType> actual = new ArrayList<>();
		parse(buf, actual);
		assertEquals(expected.size(), actual.size());
	}
}