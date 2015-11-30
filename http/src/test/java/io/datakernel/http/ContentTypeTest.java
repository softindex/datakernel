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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ContentTypeTest {
	@Test
	public void testParse() {
		String string = "text/plain, image/bmp;q=0.9, application/ogg;charset=utf-8";
		List<ContentType> types = new ArrayList<>();
		ContentType.parse(ByteBufStrings.encodeAscii(string), 0, string.length(), types);
		assertEquals(3, types.size());
	}

	@Test
	public void testRender() throws Exception {
		String expected = "text/plain,image/bmp,application/ogg";
		List<ContentType> types = Arrays.asList(ContentType.PLAIN_TEXT, ContentType.BMP, ContentType.OGG_APP);
		ByteBuf buf = ByteBuf.allocate(expected.length());
		ContentType.render(types, buf.array(), buf.position());
		assertEquals(expected, ByteBufStrings.decodeAscii(buf.array()));
	}
}
