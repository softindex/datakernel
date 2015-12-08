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

public class ConfigTest {

	@Test
	public void rootConfigDoesntHaveKey() {
		Config root = new Config();

		assertEquals("", root.getKey());
	}

	@Test
	public void configShouldStoreInfoAboutChildConfigs() {
		Config root = new Config();
		root.set("name1", "value1");
		root.set("name2", "value2");

		assertEquals("value1", root.getChild("name1").get());
		assertEquals("value2", root.getChild("name2").get());
	}

	@Test
	public void configShouldProperlySplitChildKey() {
		Config root = new Config();
		root.set("base.sub1.sub2", "value");

		Config base = root.getChild("base");
		Config sub1 = base.getChild("sub1");
		Config sub2 = sub1.getChild("sub2");

		assertNull(root.get());
		assertNull(base.get());
		assertNull(sub1.get());
		assertEquals("value", sub2.get());
	}

	@Test
	public void configShouldUpdateValueIfKeyWasAlreadyUsed() {
		Config root = new Config();
		root.set("key1", "value1");

		root.set("key1", "value2");

		assertEquals("value2", root.getChild("key1").get());
	}
}
