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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static io.datakernel.config.ConfigConverters.*;

public class ConfigsModuleTest {
	private static class TestClass {
		int field1;
		double field2;
		boolean field3;

		TestClass() {
		}

		TestClass(int field1, double field2, boolean field3) {
			this.field1 = field1;
			this.field2 = field2;
			this.field3 = field3;
		}

		@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
		@Override
		public boolean equals(Object o) {
			TestClass testClass = (TestClass) o;
			return field1 == testClass.field1
					&& Double.compare(testClass.field2, field2) == 0
					&& field3 == testClass.field3;
		}
	}

	@Test
	public void testConfigs() throws IOException {
		Properties properties1 = new Properties();
		properties1.put("port", "1234");
		properties1.put("msg", "Test phrase");
		properties1.put("innerClass.field1", "2");
		properties1.put("innerClass.field2", "3.5");

		Properties properties2 = new Properties();
		properties2.put("workers", "4");
		properties2.put("innerClass.field3", "true");

		ConfigConverter<TestClass> configConverter = new ConfigConverter<TestClass>() {
			@Override
			public TestClass get(Config config) {
				return get(config, null);
			}

			@Override
			public TestClass get(Config config, TestClass defaultValue) {
				TestClass testClass = new TestClass();
				testClass.field1 = config.get(ofInteger(), "field1");
				testClass.field2 = config.get(ofDouble(), "field2");
				testClass.field3 = config.get(ofBoolean(), "field3");
				return testClass;
			}
		};
		Config config = ConfigModule.create(
				Config.create()
						.override(Config.ofProperties(properties1))
						.override(Config.ofProperties(properties2))
						.override(Config.ofProperties("not-existing.properties", true)))
				.saveEffectiveConfigTo("resulting.properties")
				.provideConfig();

		Assert.assertEquals(1234, (int) config.get(ofInteger(), "port"));
		Assert.assertEquals("Test phrase", config.get("msg"));
		Assert.assertEquals(new TestClass(2, 3.5, true), config.get(configConverter, "innerClass"));
	}
}
