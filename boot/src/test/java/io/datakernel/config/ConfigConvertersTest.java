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

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class ConfigConvertersTest {

	@Test
	public void testBooleanConvertor() {
		ConfigConverter<Boolean> booleanConverter = ConfigConverters.ofBoolean();
		String inputString1 = "true";
		String inputString2 = "false";
		Config config1 = new Config();
		config1.set(inputString1);
		Config config2 = new Config();
		config2.set(inputString2);

		assertTrue(booleanConverter.get(config1));
		assertFalse(booleanConverter.get(config2));
	}

	@Test
	public void testIntegerConvertor() {
		ConfigConverter<Integer> integerConverter = ConfigConverters.ofInteger();
		String inputString1 = "1";
		String inputString2 = "-5";
		String inputString3 = "100";
		Config root = new Config();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(1, (int) integerConverter.get(root.getChild("key1")));
		assertEquals(-5, (int) integerConverter.get(root.getChild("key2")));
		assertEquals(100, (int) integerConverter.get(root.getChild("key3")));
	}

	@Test
	public void testLongConvertor() {
		ConfigConverter<Long> longConverter = ConfigConverters.ofLong();
		String inputString1 = "1";
		String inputString2 = "-5";
		String inputString3 = "100";
		Config root = new Config();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(1L, (long) longConverter.get(root.getChild("key1")));
		assertEquals(-5L, (long) longConverter.get(root.getChild("key2")));
		assertEquals(100L, (long) longConverter.get(root.getChild("key3")));
	}

	@Test
	public void testEnumConvertor() {
		ConfigConverter<Color> enumConverter = ConfigConverters.ofEnum(Color.class);
		String inputString1 = "RED";
		String inputString2 = "GREEN";
		String inputString3 = "BLUE";
		Config root = new Config();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(Color.RED, enumConverter.get(root.getChild("key1")));
		assertEquals(Color.GREEN, enumConverter.get(root.getChild("key2")));
		assertEquals(Color.BLUE, enumConverter.get(root.getChild("key3")));
	}

	public enum Color {
		RED, GREEN, BLUE
	}

	@Test
	public void testStringConverter() {
		ConfigConverter<String> stringConverter = ConfigConverters.ofString();
		String inputString1 = "data1";
		String inputString2 = "data2";
		String inputString3 = "data3";
		Config root = new Config();
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(inputString1, stringConverter.get(root.getChild("key1")));
		assertEquals(inputString2, stringConverter.get(root.getChild("key2")));
		assertEquals(inputString3, stringConverter.get(root.getChild("key3")));
	}

	@Test
	public void testDoubleConverter() {
		ConfigConverter<Double> doubleConverter = ConfigConverters.ofDouble();
		String inputString1 = "0.001";
		String inputString2 = "1e5";
		String inputString3 = "-23.1";
		Config root = new Config();
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
		Config root = new Config();
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
		String inputData = "1, 5,10";
		Config root = new Config();
		root.set("key1", inputData);

		assertEquals(asList(1, 5, 10), listConverter.get(root.getChild("key1")));
	}
}
