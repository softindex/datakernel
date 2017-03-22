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

package io.datakernel.stream.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.exception.ParseException;
import io.datakernel.stream.net.MessagingSerializers.ByteBufPoolAppendable;
import org.junit.Test;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static org.junit.Assert.assertEquals;

public class ByteBufPoolAppendableTest {
	private static final String HELLO_WORLD = "Hello, World!";

	@Test
	public void testAppendSimple() {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
		appendable.append(HELLO_WORLD);
		ByteBuf buf = appendable.get();
		assertEquals(0, buf.readPosition());
		assertEquals(13, buf.writePosition());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testAppendWithResizing() throws ParseException {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable(8);
		appendable.append(HELLO_WORLD);
		ByteBuf buf = appendable.get();
		assertEquals(0, buf.readPosition());
		assertEquals(13, buf.writePosition());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}