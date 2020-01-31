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

import io.datakernel.common.parse.ParseException;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.time.Month.JANUARY;
import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class HttpDateTest {

	@Test
	public void testRender() {
		LocalDate date = LocalDate.of(2015, JANUARY, 1);
		byte[] bytes = new byte[29];
		int end = HttpDate.render(date.atStartOfDay().toInstant(UTC).getEpochSecond(), bytes, 0);
		assertEquals(29, end);
		assertArrayEquals("Thu, 01 Jan 2015 00:00:00 GMT".getBytes(ISO_8859_1), bytes);
	}

	@Test
	public void testParser() throws ParseException {
		String date = "Thu, 01 Jan 2015 02:00:00 GMT";
		long actual = HttpDate.parse(date.getBytes(ISO_8859_1), 0);

		LocalDateTime dateTime = LocalDateTime.of(2015, JANUARY, 1, 2, 0);
		assertEquals(actual, dateTime.toInstant(UTC).getEpochSecond());
	}

	@Test
	public void testFull() throws ParseException {
		byte[] bytes = new byte[29];
		HttpDate.render(4073580000L, bytes, 0);
		assertEquals(4073580000L, HttpDate.parse(bytes, 0));
	}

	@Test
	public void testDateWithShortYear() throws ParseException {
		String date = "Thu, 01 Jan 15 00:00:00 GMT";
		long actual = HttpDate.parse(date.getBytes(ISO_8859_1), 0);

		LocalDate expected = LocalDate.of(2015, JANUARY, 1);
		assertEquals(expected.atStartOfDay().toInstant(UTC).getEpochSecond(), actual);
	}
}
