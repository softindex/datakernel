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
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.config.Configs.emptyConfig;
import static org.junit.Assert.*;

public class ConfigConvertersTest {
	@Test
	public void testBaseGet() {
		Map<String, Config> map = new HashMap<>();
		map.put("key1", Configs.ofValue("data1"));
		map.put("key2", Configs.ofValue("data2"));
		map.put("key3", Configs.ofValue("data3"));
		Config root = Configs.ofMap(map);

		assertEquals("data1", root.get("key1"));
		assertEquals("data2", root.get("key2"));
		assertEquals("data3", root.get("key3"));
	}

	@Test
	public void testBooleanConverter() {
		Config config1 = Configs.ofValue("true");
		Config config2 = Configs.ofValue("false");

		assertTrue(config1.get(ofBoolean(), THIS));
		assertFalse(config2.get(ofBoolean(), THIS));
	}

	@Test
	public void testIntegerConverter() {
		Map<String, Config> values = new HashMap<>();
		values.put("key1", Configs.ofValue("1"));
		values.put("key2", Configs.ofValue("-5"));
		values.put("key3", Configs.ofValue("100"));
		Config root = Configs.ofMap(values);

		assertEquals(1, (int) root.get(ofInteger(), "key1"));
		assertEquals(-5, (int) root.get(ofInteger(), "key2"));
		assertEquals(100, (int) root.get(ofInteger(), "key3"));
	}

	@Test
	public void testLongConverter() {
		Map<String, Config> values = new HashMap<>();
		values.put("key1", Configs.ofValue("1"));
		values.put("key2", Configs.ofValue("-5"));
		values.put("key3", Configs.ofValue("100"));
		Config root = Configs.ofMap(values);

		assertEquals(1L, (long) root.get(ofLong(), "key1"));
		assertEquals(-5L, (long) root.get(ofLong(), "key2"));
		assertEquals(100L, (long) root.get(ofLong(), "key3"));
	}

	private enum Color {
		RED, GREEN, BLUE
	}

	@Test
	public void testEnumConverter() {
		ConfigConverter<Color> enumConverter = ConfigConverters.ofEnum(Color.class);
		Map<String, Config> values = new HashMap<>();
		values.put("key1", Configs.ofValue("RED"));
		values.put("key2", Configs.ofValue("GREEN"));
		values.put("key3", Configs.ofValue("BLUE"));
		Config root = Configs.ofMap(values);

		assertEquals(Color.RED, root.get(enumConverter, "key1"));
		assertEquals(Color.GREEN, root.get(enumConverter, "key2"));
		assertEquals(Color.BLUE, root.get(enumConverter, "key3"));
	}

	@Test
	public void testDoubleConverter() {
		ConfigConverter<Double> doubleConverter = ConfigConverters.ofDouble();
		Map<String, Config> map = new HashMap<>();
		map.put("key1", Configs.ofValue("0.001"));
		map.put("key2", Configs.ofValue("1e5"));
		map.put("key3", Configs.ofValue("-23.1"));
		Config root = Configs.ofMap(map);

		double acceptableError = 1e-10;
		assertEquals(0.001, doubleConverter.get(root.getChild("key1")), acceptableError);
		assertEquals(1e5, doubleConverter.get(root.getChild("key2")), acceptableError);
		assertEquals(-23.1, doubleConverter.get(root.getChild("key3")), acceptableError);
	}

	@Test
	public void testInetAddressConverter() throws UnknownHostException {
		ConfigConverter<InetSocketAddress> inetSocketAddressConverter = ConfigConverters.ofInetSocketAddress();
		Map<String, Config> map = new HashMap<>();
		map.put("key1", Configs.ofValue("192.168.1.1:80"));
		map.put("key2", Configs.ofValue("250.200.100.50:10000"));
		map.put("key3", Configs.ofValue("1.0.0.0:65000"));

		Config root = Configs.ofMap(map);

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
		Config root = Configs.ofValue("1, 5,   10   ");

		List<Integer> expected = Arrays.asList(1, 5, 10);
		assertEquals(expected, listConverter.get(root));
	}

	@Test
	public void testInetAddressRange() throws Exception {
		ConfigConverter<InetAddressRange> rangeConverter = ConfigConverters.ofInetAddressRange();
		Config root = Configs.ofValue("192.168.0.0/16");

		InetAddressRange expected = InetAddressRange.fromRange(
				(Inet4Address) InetAddress.getByName("192.168.0.0"),
				(Inet4Address) InetAddress.getByName("192.168.255.255")
		);
		assertEquals(expected, rangeConverter.get(root));
	}

	@Test
	public void testDefaultNullValues() throws UnknownHostException {
		Integer value = ofInteger().get(emptyConfig(), null);
		assertNull(value);
	}

	@Test
	public void testDatagraphSocketSettingsConverter() throws UnknownHostException {
		Config emptyConfig = emptyConfig();
		DatagramSocketSettings expected = DatagramSocketSettings.create()
				.withReceiveBufferSize(MemSize.bytes(256))
				.withSendBufferSize(1024)
				.withReuseAddress(false)
				.withBroadcast(true);

		DatagramSocketSettings actual = emptyConfig.get(ofDatagramSocketSettings(), THIS, expected);

		assertEquals(expected.getBroadcast(), actual.getBroadcast());
		assertEquals(expected.getReuseAddress(), actual.getReuseAddress());
		assertEquals(expected.getReceiveBufferSize(), actual.getReceiveBufferSize());
		assertEquals(expected.getSendBufferSize(), actual.getSendBufferSize());
	}

	@Test
	public void testServerSocketSettings() {
		Config emptyConfig = emptyConfig();
		ServerSocketSettings expected = ServerSocketSettings.create(1)
				.withReceiveBufferSize(64)
				.withReuseAddress(true);

		ServerSocketSettings actual = emptyConfig.get(ofServerSocketSettings(), THIS, expected);
		assertEquals(expected.getBacklog(), actual.getBacklog());
		assertEquals(expected.getReceiveBufferSize(), actual.getReceiveBufferSize());
		assertEquals(expected.getReuseAddress(), actual.getReuseAddress());
	}

	@Test
	public void testSocketSettings() {
		Config emptyConfig = emptyConfig();
		SocketSettings expected = SocketSettings.create()
				.withTcpNoDelay(true)
				.withReuseAddress(false)
				.withReceiveBufferSize(256)
				.withSendBufferSize(512)
				.withKeepAlive(true);

		SocketSettings actual = emptyConfig.get(ofSocketSettings(), THIS, expected);

		assertFalse(actual.hasImplReadSize());
		assertFalse(actual.hasImplWriteSize());

		assertEquals(expected.getTcpNoDelay(), actual.getTcpNoDelay());
		assertEquals(expected.getReuseAddress(), actual.getReuseAddress());
		assertEquals(expected.getReceiveBufferSize(), actual.getReceiveBufferSize());
		assertEquals(expected.getSendBufferSize(), actual.getSendBufferSize());
		assertEquals(expected.getKeepAlive(), actual.getKeepAlive());
	}
}
