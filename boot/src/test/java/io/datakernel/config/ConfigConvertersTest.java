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

import com.google.common.reflect.TypeToken;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

import static io.datakernel.config.ConfigConverters.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class ConfigConvertersTest {

	@Test
	public void testBooleanConverter() {
		ConfigConverter<Boolean> booleanConverter = ofBoolean();
		String inputString1 = "true";
		String inputString2 = "false";
		ConfigTree config1 = ConfigTree.newInstance().registerConverter(Boolean.class, ofBoolean());
		config1.set(inputString1);
		ConfigTree config2 = ConfigTree.newInstance().registerConverter(Boolean.class, ofBoolean());
		config2.set(inputString2);

		assertTrue(booleanConverter.get(config1));
		assertFalse(booleanConverter.get(config2));
	}

	@Test
	public void testIntegerConverter() {
		ConfigConverter<Integer> integerConverter = ConfigConverters.ofInteger();
		String inputString1 = "1";
		String inputString2 = "-5";
		String inputString3 = "100";
		ConfigTree root = ConfigTree.newInstance().registerConverter(Integer.class, ofInteger());
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(1, (int) integerConverter.get((ConfigTree) root.getChild("key1")));
		assertEquals(-5, (int) integerConverter.get((ConfigTree) root.getChild("key2")));
		assertEquals(100, (int) integerConverter.get((ConfigTree) root.getChild("key3")));
	}

	@Test
	public void testLongConverter() {
		ConfigConverter<Long> longConverter = ConfigConverters.ofLong();
		String inputString1 = "1";
		String inputString2 = "-5";
		String inputString3 = "100";
		ConfigTree root = ConfigTree.newInstance().registerConverter(Long.class, ofLong());
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(1L, (long) longConverter.get((ConfigTree) root.getChild("key1")));
		assertEquals(-5L, (long) longConverter.get((ConfigTree) root.getChild("key2")));
		assertEquals(100L, (long) longConverter.get((ConfigTree) root.getChild("key3")));
	}

	@Test
	public void testEnumConverter() {
		ConfigConverter<Color> enumConverter = ConfigConverters.ofEnum(Color.class);
		String inputString1 = "RED";
		String inputString2 = "GREEN";
		String inputString3 = "BLUE";
		ConfigTree root = ConfigTree.newInstance().registerConverter(Color.class, ofEnum(Color.class));
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(Color.RED, enumConverter.get((ConfigTree) root.getChild("key1")));
		assertEquals(Color.GREEN, enumConverter.get((ConfigTree) root.getChild("key2")));
		assertEquals(Color.BLUE, enumConverter.get((ConfigTree) root.getChild("key3")));
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
		ConfigTree root = ConfigTree.newInstance().registerConverter(String.class, ofString());
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		assertEquals(inputString1, stringConverter.get((ConfigTree) root.getChild("key1")));
		assertEquals(inputString2, stringConverter.get((ConfigTree) root.getChild("key2")));
		assertEquals(inputString3, stringConverter.get((ConfigTree) root.getChild("key3")));
	}

	@Test
	public void testDoubleConverter() {
		ConfigConverter<Double> doubleConverter = ConfigConverters.ofDouble();
		String inputString1 = "0.001";
		String inputString2 = "1e5";
		String inputString3 = "-23.1";
		ConfigTree root = ConfigTree.newInstance().registerConverter(Double.class, ofDouble());
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		double acceptableError = 1e-10;
		assertEquals(0.001, doubleConverter.get((ConfigTree) root.getChild("key1")), acceptableError);
		assertEquals(1e5, doubleConverter.get((ConfigTree) root.getChild("key2")), acceptableError);
		assertEquals(-23.1, doubleConverter.get((ConfigTree) root.getChild("key3")), acceptableError);
	}

	@Test
	public void testInetAddressConverter() throws UnknownHostException {
		ConfigConverter<InetSocketAddress> inetSocketAddressConverter = ConfigConverters.ofInetSocketAddress();
		String inputString1 = "192.168.1.1:80";
		String inputString2 = "250.200.100.50:10000";
		String inputString3 = "1.0.0.0:65000";
		ConfigTree root = ConfigTree.newInstance().registerConverter(InetSocketAddress.class, inetSocketAddressConverter);
		root.set("key1", inputString1);
		root.set("key2", inputString2);
		root.set("key3", inputString3);

		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("192.168.1.1"), 80);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("250.200.100.50"), 10000);
		InetSocketAddress address3 = new InetSocketAddress(InetAddress.getByName("1.0.0.0"), 65000);
		assertEquals(address1, inetSocketAddressConverter.get((ConfigTree) root.getChild("key1")));
		assertEquals(address2, inetSocketAddressConverter.get((ConfigTree) root.getChild("key2")));
		assertEquals(address3, inetSocketAddressConverter.get((ConfigTree) root.getChild("key3")));
	}

	@Test
	public void testListConverter() {
		ConfigConverter<List<Integer>> listConverter = ConfigConverters.ofList(ConfigConverters.ofInteger(), ",");
		String inputData = "1, 5,10";
		ConfigTree root = ConfigTree.newInstance().registerConverter(new TypeToken<List<Integer>>() {}, listConverter);
		root.set("key1", inputData);

		assertEquals(asList(1, 5, 10), listConverter.get((ConfigTree) root.getChild("key1")));
	}
}
