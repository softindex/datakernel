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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.config.Config.ROOT;
import static io.datakernel.config.ConfigConverters.ofString;
import static org.junit.Assert.*;

public class ConfigTest {

	@Rule
	public ExpectedException expected = ExpectedException.none();

	@Test
	public void rootConfigDoesNotHaveKey() {
		Config root = Config.create();
		assertEquals("", root.getKey());
	}

	@Test
	public void configShouldStoreInfoAboutChildConfigs() {
		Config root = Config.create();
		root.set("name1", "value1");
		root.set("name2", "value2");

		assertEquals("value1", root.getChild("name1").get(ofString(), ROOT));
		assertEquals("value2", root.getChild("name2").get(ofString(), ROOT));
	}

	@Test
	public void configShouldProperlySplitChildKey() {
		Config root = Config.create();
		root.set("base.sub1.sub2", "value");

		Config base = (Config) root.getChild("base");
		Config sub1 = (Config) base.getChild("sub1");
		Config sub2 = (Config) sub1.getChild("sub2");

		assertNull(root.get());
		assertNull(base.get());
		assertNull(sub1.get());
		assertEquals("value", sub2.get());
	}

	@Test
	public void configShouldUpdateValueIfKeyWasAlreadyUsed() {
		Config root = Config.create();
		root.set("key1", "value1");

		root.set("key1", "value2");

		assertEquals("value2", root.getChild("key1").get(ofString(), ROOT));
	}

	@Test
	public void testConfigWithValueCanHaveNoChildren() {
		Properties properties = new Properties();
		properties.setProperty("a.b.c", "a.b.c");
		Config config = Config.ofProperties(properties);

		config.get(ofString(), "a.b.c.d", "a.b.c.d");
		expected.expect(IllegalStateException.class);
		config.get(ofString(), "a.b.c");
	}

	@Ignore("stress test to find out whether config works correctly in multithreaded environment")
	@Test
	public void testStressSynchronizedGetKey() {
		// IllegalStateException is ok, when adding default value

		ExecutorService executor = Executors.newFixedThreadPool(200);
		final List<String> existingPaths = new ArrayList<>();
		final Random random = new Random(2L);

		Properties properties = createRandomProperties(random, existingPaths, 100000);
		final Config config = Config.ofProperties(properties);

		final ConcurrentHashMap<String, Throwable> exceptions = new ConcurrentHashMap<>();

		for (int i = 0; i < 16000; i++) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						Thread.sleep(random.nextInt(10));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					String path = existingPaths.get(random.nextInt(existingPaths.size()));
					try {
						config.get(ofString(), path, "DefaultValue");
					} catch (Exception e) {
						e.printStackTrace();
						exceptions.put(path, e);
					}
				}
			});
		}
		executor.shutdown();
		assertTrue(exceptions.isEmpty());
	}

	private Properties createRandomProperties(Random random, Collection<String> existingPaths, int n) {
		Properties properties = new Properties();
		List<Character> alphabet = new ArrayList<>();
		int maxPathLength = 5;
		for (int i = 65; i <= 90; i++) {
			alphabet.add((char) i);
		}
		int fails = 0;
		for (int i = 0; i < n; ) {
			String path = "";
			int length = random.nextInt(maxPathLength) + 1;
			for (int j = 0; j < length; j++) {
				path += alphabet.get(random.nextInt(alphabet.size()));
				if (j != (length - 1)) {
					path += ".";
				}
			}
			boolean exists = false;
			for (String s : existingPaths) {
				if (s.startsWith(path) || path.startsWith(s)) {
					exists = true;
					break;
				}
			}
			existingPaths.add(path);
			if (!exists) {
				if (random.nextBoolean()) {
					properties.setProperty(path, path);
				}
				i++;
				fails = 0;
			} else {
				if (++fails > 10) {
					break;
				}
			}
		}
		return properties;
	}
}
