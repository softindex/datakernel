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

import io.datakernel.config.impl.TreeConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.datakernel.config.ConfigConverters.ofByte;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigTestUtils.testBaseConfig;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

public class TreeConfigTest {
	private TreeConfig config;

	@Before
	public void setUp() {
		config = TreeConfig.ofTree();
		config.addLeaf("key1", "value1");
		config.addBranch("key2.key3").addLeaf("key4", "value4");
		config.withValue("key5.key6", "6");
	}

	@Test(expected = IllegalStateException.class)
	public void testLeafOnLeafAdding() {
		config.withValue("key1.key2", "invalidValue");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCanNotAddBadPath() {
		config.withValue("a..b.", "illegalValue");
	}

	@Test
	public void testBase() {
		TreeConfig config = TreeConfig.ofTree()
				.withValue("a.a.a", "1")
				.withValue("a.a.b", "2")
				.withValue("a.b", "3")
				.withValue("b", "4");
		testBaseConfig(config);
	}

	@Test
	public void testWorksProperlyWithChildren() {
		assertTrue(config.hasChild("key2.key3"));
		Config child = config.getChild("key2.key3");
		assertNotNull(child);
		assertEquals(singleton("key4"), child.getChildren());
		assertEquals(Stream.of("key1", "key2", "key5").collect(Collectors.toSet()), config.getChildren());

		assertEquals(
				config.getChild("key2.key3.key4"),
				config.getChild("key2").getChild("key3").getChild("key4")
		);
	}

	@Test
	public void testWorksWithDefaultValues() {
		TreeConfig root = TreeConfig.ofTree();
		Integer value = root.get(ofInteger(), "not.existing.branch", 8080);
		assertEquals(8080, (int) value);
	}

	@Test(expected = NoSuchElementException.class)
	public void testShouldThrowNoSuchElementIfMissing() {
		config.get("a.b.c.d");
	}

	@Test
	public void testWorksProperlyWithConverters() {
		Byte value = config.get(ofByte(), "key5.key6");
		assertEquals((byte) 6, (byte) value);
	}
}