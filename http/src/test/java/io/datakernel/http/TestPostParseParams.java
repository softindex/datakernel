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

import java.util.Map;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static org.junit.Assert.assertEquals;

public class TestPostParseParams {
	@Test
	public void testParameters() throws ParseException {
		ByteBuf body = wrapAscii("hello=world&value=1234");

		HttpRequest request = HttpRequest.post("http://127.0.0.1")
				.withHeader(CONTENT_TYPE, encodeAscii("application/x-www-form-urlencoded"))
				.withBody(body);

		Map<String, String> params = request.getPostParameters();

		assertEquals(2, params.size());
		assertEquals("world", params.get("hello"));
		assertEquals("1234", params.get("value"));
		body.recycle();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
