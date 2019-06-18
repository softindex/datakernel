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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.test.rules.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static org.junit.Assert.assertEquals;

public final class TestPostParseParams {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testParameters() {
		ByteBuf body = wrapAscii("hello=world&value=1234");

		Map<String, String> params = UrlParser.parseQueryIntoMap(decodeAscii(body.array(), body.head(), body.readRemaining()));

		assertEquals(2, params.size());
		assertEquals("world", params.get("hello"));
		assertEquals("1234", params.get("value"));

		body.recycle();
	}
}
