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

import io.datakernel.net.ServerSocketSettings;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ConfigTest {
	@Test
	public void testTrim() {
		Map<String, Config> map = new HashMap<>();
		map.put("a", Config.ofValue(" value "));
		map.put("b", Config.ofValue("value "));
		Config config = Config.ofChildren(map);
		assertEquals("value", config.get("a"));
		assertEquals("value", config.get("b"));
	}

	@Test
	public void testOfConverter1() {
		Config config = Config.ofValue(ConfigConverters.ofServerSocketSettings(), ServerSocketSettings.create(16384));
		assertEquals("16384", config.get("backlog"));
		assertTrue(config.hasChild("receiveBufferSize"));
		assertEquals("X", config.get("receiveBufferSize", "X"));
	}

	@Test
	public void testOfConverter2() {
		Config config = Config.ofValue(ConfigConverters.ofLong(), 0L);
		assertEquals("0", config.getValue());
	}
}