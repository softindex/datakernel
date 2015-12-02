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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CharsetUtilsTest {
	@Test
	public void testCharsetRender() {
		List<HttpUtils.Pair<Charset>> charsets = new ArrayList<>();
		charsets.add(new HttpUtils.Pair<>(Charset.forName("UTF-8"), 0.7));
		charsets.add(new HttpUtils.Pair<>(Charsets.ISO_8859_1));
		charsets.add(new HttpUtils.Pair<>(Charset.forName("US-ASCII"), 0.7));
		charsets.add(new HttpUtils.Pair<>(Charset.forName("UTF-16"), 0.9));

		ByteBuf buf = ByteBuf.allocate(53);
		CharsetUtils.render(charsets, buf);

		String expected = "utf-8;q=0.7, iso-8859-1, us-ascii;q=0.7, utf-16;q=0.9";
		assertEquals(expected, ByteBufStrings.decodeAscii(buf.array()));
	}

	@Test
	public void testParser() {
		List<HttpUtils.Pair<Charset>> actual = new ArrayList<>();
		String string = "utf-8;q=0.7, iso-8859-1, us-ascii;q=0.7, utf-16;q=0.9";
		CharsetUtils.parse(ByteBufStrings.encodeAscii(string), 0, string.length(), actual);

		assertEquals(4, actual.size());
	}

	@Test
	public void testParseUnknown() {
		List<HttpUtils.Pair<Charset>> actual = new ArrayList<>();
		String string = "someUnsupportedCharset";
		CharsetUtils.parse(ByteBufStrings.encodeAscii(string), 0, string.length(), actual);
		assertEquals(0, actual.size());
	}

}
