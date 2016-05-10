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

package io.datakernel.config;

import io.datakernel.eventloop.InetAddressRange;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

import static io.datakernel.config.Config.ROOT;
import static io.datakernel.config.ConfigConverters.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class ConfigConvertersTest {

	@Test
	public void testBooleanConverter() {
		String inputString1 = "true";
		String inputString2 = "false";
		Config config1 = Config.create();
		config1.set(inputString1);
		Config config2 = Config.create();
		config2.set(inputString2);

		assertTrue(config1.get(ofBoolean(), ROOT));
		assertFalse(config2.get(ofBoolean(), ROOT));
	}

	@Test
	public void testIntegerConverter() {
		String inputString1 = "1";
		String inputString2 = "-5";
		String inputString3 = "100";
		Config root = Config.create();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(1, (int) root.get(ofInteger(), "key1"));
		assertEquals(-5, (int) root.get(ofInteger(), "key2"));
		assertEquals(100, (int) root.get(ofInteger(), "key3"));
	}

	@Test
	public void testLongConverter() {
		String inputString1 = "1";
		String inputString2 = "-5";
		String inputString3 = "100";
		Config root = Config.create();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(1L, (long) root.get(ofLong(), "key1"));
		assertEquals(-5L, (long) root.get(ofLong(), "key2"));
		assertEquals(100L, (long) root.get(ofLong(), "key3"));
	}

	@Test
	public void testEnumConverter() {
		ConfigConverter<Color> enumConverter = ConfigConverters.ofEnum(Color.class);
		String inputString1 = "RED";
		String inputString2 = "GREEN";
		String inputString3 = "BLUE";
		Config root = Config.create();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(Color.RED, root.get(enumConverter, "key1"));
		assertEquals(Color.GREEN, root.get(enumConverter, "key2"));
		assertEquals(Color.BLUE, root.get(enumConverter, "key3"));
	}

	private enum Color {
		RED, GREEN, BLUE
	}

	@Test
	public void testStringConverter() {
		ConfigConverter<String> stringConverter = ConfigConverters.ofString();
		String inputString1 = "data1";
		String inputString2 = "data2";
		String inputString3 = "data3";
		Config root = Config.create();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(inputString1, root.get(stringConverter, "key1"));
		assertEquals(inputString2, root.get(stringConverter, "key2"));
		assertEquals(inputString3, root.get(stringConverter, "key3"));
	}

	@Test
	public void testDoubleConverter() {
		ConfigConverter<Double> doubleConverter = ConfigConverters.ofDouble();
		String inputString1 = "0.001";
		String inputString2 = "1e5";
		String inputString3 = "-23.1";
		Config root = Config.create();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		double acceptableError = 1e-10;
		assertEquals(0.001, doubleConverter.get(root.getChild("key1")), acceptableError);
		assertEquals(1e5, doubleConverter.get(root.getChild("key2")), acceptableError);
		assertEquals(-23.1, doubleConverter.get(root.getChild("key3")), acceptableError);
	}

	@Test
	public void testInetAddressConverter() throws UnknownHostException {
		ConfigConverter<InetSocketAddress> inetSocketAddressConverter = ConfigConverters.ofInetSocketAddress();
		String inputString1 = "192.168.1.1:80";
		String inputString2 = "250.200.100.50:10000";
		String inputString3 = "1.0.0.0:65000";
		Config root = Config.create();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("192.168.1.1"), 80);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("250.200.100.50"), 10000);
		InetSocketAddress address3 = new InetSocketAddress(InetAddress.getByName("1.0.0.0"), 65000);
		assertEquals(address1, inetSocketAddressConverter.get(root.getChild("key1")));
		assertEquals(address2, inetSocketAddressConverter.get(root.getChild("key2")));
		assertEquals(address3, inetSocketAddressConverter.get(root.getChild("key3")));
	}

	@Test
	public void testListConverter() {
		ConfigConverter<List<Integer>> listConverter = ConfigConverters.ofList(ConfigConverters.ofInteger(), ",");
		String inputData = "1, 5,   10   ";
		Config root = Config.create();
		root.set("key1", inputData);

		List<Integer> expected = asList(1, 5, 10);
		assertEquals(expected, listConverter.get(root.getChild("key1")));
	}

	@Test
	public void testInetAddressRange() throws Exception {
		ConfigConverter<InetAddressRange> rangeConverter = ConfigConverters.ofInetAddressRange();
		String inputData = "192.168.0.0/16";
		Config root = Config.create();
		root.set("key1", inputData);

		InetAddressRange expected = InetAddressRange.fromRange(
				(Inet4Address) InetAddress.getByName("192.168.0.0"),
				(Inet4Address) InetAddress.getByName("192.168.255.255")
		);
		assertEquals(expected, rangeConverter.get(root.getChild("key1")));
	}
}
