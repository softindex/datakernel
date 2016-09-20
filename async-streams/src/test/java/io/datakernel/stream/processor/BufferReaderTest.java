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

package io.datakernel.stream.processor;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BufferReaderTest {

	public static final int CHAR_START = 3;

	private void doTest(String str) throws IOException {
		byte[] buf = new byte[100];
		BufferAppendable appendable = BufferAppendable.create(buf, 10);
		appendable.append(str);
		BufferReader reader = BufferReader.create(buf, 10, appendable.position() - 10);
		char[] chars = new char[100];
		int charPos = CHAR_START;
		int numRead = reader.read(chars, charPos, 1);
		if (numRead != -1) {
			charPos += numRead;
			numRead = reader.read(chars, charPos, 2);
		}
		if (numRead != -1) {
			charPos += numRead;
			numRead = reader.read(chars, charPos, 2);
		}
		if (numRead != -1) {
			charPos += numRead;
			numRead = reader.read(chars, charPos, chars.length - charPos);
		}
		if (numRead != -1) {
			charPos += numRead;
			numRead = reader.read(chars, charPos, chars.length - charPos);
		}
		Assert.assertEquals(-1, numRead);
		String actual = String.copyValueOf(chars, CHAR_START, charPos - CHAR_START);
		Assert.assertEquals(str, actual);
	}

	@Test
	public void testRead() throws Exception {
		doTest("");
		doTest("Test");
		doTest("Тест");
	}
}