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

package io.datakernel.utils;

import org.junit.Test;

import static io.datakernel.http.HttpUtils.isInetAddress;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestUtils {
	@Test
	public void testIsIpv4InetAddress() {
		String ip = "127.0.0.1";
		assertTrue(isInetAddress(ip));

		ip = ".127.0.0";
		assertFalse(isInetAddress(ip));

		ip = "255.255.255.255";
		assertTrue(isInetAddress(ip));

		ip = "0.0.0.0";
		assertTrue(isInetAddress(ip));

		ip = "345.213.2344.78568";
		assertFalse(isInetAddress(ip));

		ip = "11.11..11";
		assertFalse(isInetAddress(ip));

		ip = "11.";
		assertFalse(isInetAddress(ip));
	}

	@Test
	public void testIsIpv6InetAddress() {
		String ip = "FEDC:BA98:7654:3210:FEDC:BA98:7654:3210";
		assertTrue(isInetAddress(ip));

		ip = "f:0:e:0:A:0:C:0";
		assertTrue(isInetAddress(ip));

		ip = "::3210";
		assertTrue(isInetAddress(ip));

		ip = "::EEEE::3210";
		assertFalse(isInetAddress(ip));

		ip = "AAAAAAAA::3210";
		assertFalse(isInetAddress(ip));

		ip = "[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]";
		assertTrue(isInetAddress(ip));

		ip = "FEDC:BA98:7654:3210:FEDC:BA98:7654:3210:FEDC:BA98:7654:3210:FEDC:BA98:7654:3210";
		assertFalse(isInetAddress(ip));

		ip = "111...dfff:eeaa:";
		assertFalse(isInetAddress(ip));

		ip = "FEDC:BA98:";
		assertFalse(isInetAddress(ip));

		ip = "::127.0.0.1";
		assertTrue(isInetAddress(ip));

		ip = "0:0:0:0:0:0:13.1.68.3";
		assertTrue(isInetAddress(ip));
	}
}
