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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConfigPropertiesTest {

	@Test
	public void rootConfigDoesNotHaveKey() {
		ConfigNode root = ConfigNode.defaultInstance();

		assertEquals("", root.getKey());
	}

	@Test
	public void configShouldStoreInfoAboutChildConfigs() {
		ConfigNode root = ConfigNode.defaultInstance();
		root.set("name1", "value1");
		root.set("name2", "value2");

		assertEquals("value1", root.getChild("name1").get(String.class));
		assertEquals("value2", root.getChild("name2").get(String.class));
	}

	@Test
	public void configShouldProperlySplitChildKey() {
		ConfigNode root = ConfigNode.defaultInstance();
		root.set("base.sub1.sub2", "value");

		ConfigNode base = (ConfigNode) root.getChild("base");
		ConfigNode sub1 = (ConfigNode) base.getChild("sub1");
		ConfigNode sub2 = (ConfigNode) sub1.getChild("sub2");

		assertNull(root.get());
		assertNull(base.get());
		assertNull(sub1.get());
		assertEquals("value", sub2.get());
	}

	@Test
	public void configShouldUpdateValueIfKeyWasAlreadyUsed() {
		ConfigNode root = ConfigNode.defaultInstance();
		root.set("key1", "value1");

		root.set("key1", "value2");

		assertEquals("value2", root.getChild("key1").get(String.class));
	}
}
