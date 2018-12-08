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

package io.datakernel.eventloop;

import io.datakernel.exception.ParseException;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class InetAddressRangeTest {
	@Test
	public void testBasic() throws ParseException {
		InetAddressRange range0 = InetAddressRange.parse("192.168.0.0/16");
		InetAddressRange range1 = InetAddressRange.parse("192.168.0.0-192.168.255.255");

		assertEquals(range0, range1);
	}

	@Test
	public void testContains() throws ParseException, UnknownHostException {
		InetAddressRange range = InetAddressRange.parse("192.168.1.0/24");

		assertTrue(range.contains(Inet4Address.getByName("192.168.1.1")));
		assertFalse(range.contains(Inet4Address.getByName("192.168.2.1")));
	}

	@Test
	public void testIfSubnet() throws ParseException {
		InetAddressRange range1 = InetAddressRange.parse("192.168.1.0/24");
		InetAddressRange range2 = InetAddressRange.parse("192.168.1.21");

		assertTrue(range1.isSubnet());
		assertFalse(range2.isSubnet());
	}

	@Test
	public void testIpv6() throws UnknownHostException, ParseException {
		InetAddressRange range = InetAddressRange.fromCidr((Inet6Address) InetAddress.getByName("2001:db8::3257:9652"), 96);

		assertTrue(range.contains(InetAddress.getByName("2001:db8::11:11")));
	}

	@Test
	public void testAcceptAll() throws UnknownHostException, ParseException {
		InetAddressRange range = InetAddressRange.parse("*");

		assertTrue(range.contains(InetAddress.getByName("::")));
		assertTrue(range.contains(InetAddress.getByName("255.255.255.255")));
	}
}