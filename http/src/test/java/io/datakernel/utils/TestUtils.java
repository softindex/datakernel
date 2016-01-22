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

import io.datakernel.util.Utils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestUtils {
	@Test
	public void testIsIpv4InetAddress() {
		String ip = "127.0.0.1";
		assertTrue(Utils.isInetAddress(ip));

		ip = "255.255.255.255";
		assertTrue(Utils.isInetAddress(ip));

		ip = "0.0.0.0";
		assertTrue(Utils.isInetAddress(ip));

		ip = "345.213.2344.78568";
		assertFalse(Utils.isInetAddress(ip));
	}

	@Test
	public void testIsIpv6InetAddress() {
		String ip = "FEDC:BA98:7654:3210:FEDC:BA98:7654:3210";
		assertTrue(Utils.isInetAddress(ip));

		ip = "f:0:e:0:A:0:C:0";
		assertTrue(Utils.isInetAddress(ip));

		ip = "::3210";
//		assertTrue(Utils.isInetAddress(ip));

		ip = "[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]";
//		assertTrue(Utils.isInetAddress(ip));

		ip = "0:0:0:0:0:0:13.1.68.3";
//		assertTrue(Utils.isInetAddress(ip));
	}
}
