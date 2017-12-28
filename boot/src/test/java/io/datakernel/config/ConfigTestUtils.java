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


import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.config.Configs.EMPTY_CONFIG;
import static org.junit.Assert.*;

@SuppressWarnings("WeakerAccess")
public class ConfigTestUtils {
	/**
	 * Config of the following form is required:
	 * key: a.a.a, value: value1;
	 * key: a.a.b, value: value2;
	 * key: a.b  , value: value3;
	 * key: b    , value: value4;
	 */
	public static void testBaseConfig(Config config) {
		testHasChild(config);
		testGetChild(config);
		testGetChildren(config);

		testHasValue(config);
		testSimpleGet(config);
		testSimpleGetWithDefault(config);
		testGetWithConverter(config);
		testGetWithConverterWithDefault(config);
	}

	private static void testHasChild(Config config) {
		assertTrue(config.hasChild("a"));
		assertTrue(config.hasChild("a.a"));
		assertTrue(config.hasChild("a.a.a"));
		assertTrue(config.hasChild("a.a.b"));
		assertTrue(config.hasChild("a.b"));
		assertTrue(config.hasChild("b"));

		assertFalse(config.hasChild(""));
		assertFalse(config.hasChild("."));
		assertFalse(config.hasChild("a.a.c"));
		assertFalse(config.hasChild("a.c"));
		assertFalse(config.hasChild("c"));
	}

	private static void testGetChild(Config config) {
		assertNotNull(config.getChild("a.a.a"));
		assertNotNull(config.getChild("a.a.b"));
		assertNotNull(config.getChild("a.a"));
		assertNotNull(config.getChild("a.b"));
		assertNotNull(config.getChild("a"));
		assertNotNull(config.getChild("b"));

		assertEquals(EMPTY_CONFIG, config.getChild("a.a.c"));
		assertEquals(EMPTY_CONFIG, config.getChild("a.c"));
		assertEquals(EMPTY_CONFIG, config.getChild("c"));

		assertEquals(config.getChild(""), config);
		assertEquals(config.getChild("a").getChild("a").getChild("a"), config.getChild("a.a.a"));
		assertEquals(config.getChild("a").getChild("a").getChild("b"), config.getChild("a.a.b"));
		assertEquals(config.getChild("a").getChild("b"), config.getChild("a.b"));

		assertNotEquals(config.getChild("a").getChild("a").getChild("a"), config.getChild("a.a.b"));
	}

	private static void testGetChildren(Config config) {
		assertEquals(Stream.of("a", "b").collect(Collectors.toSet()), config.getChildren());
		assertEquals(Stream.of("a", "b").collect(Collectors.toSet()), config.getChild("a").getChildren());
		assertEquals(Stream.of("a", "b").collect(Collectors.toSet()), config.getChild("a.a").getChildren());

		assertEquals(new HashSet<>(), config.getChild("a.a.a").getChildren());
	}

	private static void testHasValue(Config config) {
		assertTrue(config.getChild("a.a.a").hasValue());
		assertTrue(config.getChild("a.a.b").hasValue());
		assertTrue(config.getChild("a.b").hasValue());
		assertTrue(config.getChild("b").hasValue());

		// existing 'branch' routes
		assertFalse(config.getChild("a.a").hasValue());
		assertFalse(config.getChild("a").hasValue());
		assertFalse(config.hasValue());

		// not existing routes
		assertFalse(config.getChild("b.a.a").hasValue());
		assertFalse(config.getChild("a.a.c").hasValue());
	}

	private static void testSimpleGet(Config config) {
		assertEquals("1", config.get("a.a.a"));
		assertEquals("2", config.get("a.a.b"));
		assertEquals("3", config.get("a.b"));
		assertEquals("4", config.get("b"));

		testBadPaths(config);
	}

	private static void testSimpleGetWithDefault(Config config) {
		String defaultValue = "defaultValue";
		assertEquals("1", config.get("a.a.a", defaultValue));
		assertEquals("2", config.get("a.a.b", defaultValue));
		assertEquals("3", config.get("a.b", defaultValue));
		assertEquals("4", config.get("b", defaultValue));

		assertEquals(defaultValue, config.get("", defaultValue));
		assertEquals(defaultValue, config.get(".", defaultValue));
		assertEquals(defaultValue, config.get("a", defaultValue));
		assertEquals(defaultValue, config.get("b.a..", defaultValue));
	}

	private static void testGetWithConverter(Config config) {
		ConfigConverter<Byte> converter = ConfigConverters.ofByte();

		assertEquals(1, (byte) config.get(converter, "a.a.a"));
		assertEquals(2, (byte) config.get(converter, "a.a.b"));
		assertEquals(3, (byte) config.get(converter, "a.b"));
		assertEquals(4, (byte) config.get(converter, "b"));

		testBadPaths(config);
	}

	private static void testGetWithConverterWithDefault(Config config) {
		ConfigConverter<Byte> converter = ConfigConverters.ofByte();
		byte defaultValue = 5;

		assertEquals(1, (byte) config.get(converter, "a.a.a", defaultValue));
		assertEquals(2, (byte) config.get(converter, "a.a.b", defaultValue));
		assertEquals(3, (byte) config.get(converter, "a.b", defaultValue));
		assertEquals(4, (byte) config.get(converter, "b", defaultValue));

		assertEquals(defaultValue, (byte) config.get(converter, "", defaultValue));
		assertEquals(defaultValue, (byte) config.get(converter, ".", defaultValue));
		assertEquals(defaultValue, (byte) config.get(converter, "a", defaultValue));
		assertEquals(defaultValue, (byte) config.get(converter, "b.a..", defaultValue));
	}

	private static void testBadPaths(Config config) {
		try {
			config.get("");
			fail("should have no value for path: \"\"");
		} catch (NoSuchElementException ignore) {
		}

		try {
			config.get(".");
			fail("should have no value for path: \".\"");
		} catch (NoSuchElementException ignore) {
		}

		try {
			config.get("a");
			fail("should have no value for path: \"a\"");
		} catch (NoSuchElementException ignore) {
		}

		try {
			config.get("a.a..");
			fail("should have no value for path \"a.a..\"");
		} catch (NoSuchElementException ignore) {
		}
	}
}
