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

import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static java.util.Calendar.JANUARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpDateTest {
	@Test
	public void testRender() {
		GregorianCalendar calendar = new GregorianCalendar(2015, JANUARY, 1);
		calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
		long timestamp = calendar.getTimeInMillis();
		byte[] bytes = new byte[29];
		int end = HttpDate.render(timestamp, bytes, 0);
		assertEquals(29, end);
		assertTrue(Arrays.equals("Thu, 01 Jan 2015 00:00:00 GMT".getBytes(Charset.forName("ISO-8859-1")), bytes));
	}

	@Test
	public void testParser() {
		String date = "Thu, 01 Jan 2015 00:00:00 GMT";
		long actual = HttpDate.parse(date.getBytes(Charset.forName("ISO-8859-1")), 0);

		GregorianCalendar calendar = new GregorianCalendar(2015, JANUARY, 1);
		calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
		long expected = calendar.getTimeInMillis();

		assertEquals(actual, expected);
	}
}
