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

public class PropertiesConfigTest {
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

		Config config = PropertiesConfig.builder()
				.addProperties(properties1)
				.addProperties(properties2)
				.registerConfigConverter(Integer.class, ofInteger())
				.registerConfigConverter(Double.class, ofDouble())
				.registerConfigConverter(Boolean.class, ofBoolean())
				.registerConfigConverter(String.class, ofString())
				.registerConfigConverter(TestClass.class, new ConfigConverter<TestClass>() {
					@Override
					public TestClass get(ConfigTree config) {
						return get(config, null);
					}

					@Override
					public TestClass get(ConfigTree config, TestClass defaultValue) {
						TestClass testClass = new TestClass();
						testClass.field1 = config.get("field1", Integer.class);
						testClass.field2 = config.get("field2", Double.class);
						testClass.field3 = config.get("field3", Boolean.class);
						return testClass;
					}

					@Override
					public void set(ConfigTree config, TestClass item) {
						config.set("field1", Integer.toString(item.field1));
						config.set("field2", Double.toString(item.field2));
						config.set("field3", Boolean.toString(item.field3));
					}
				})
				.build();

		Assert.assertEquals(1234, (int) config.get("port", Integer.class));
		Assert.assertEquals("Test phrase", config.get("msg", String.class));
		Assert.assertEquals(new TestClass(2, 3.5, true), config.get("innerClass", TestClass.class));
	}

	private static class TestClass {
		public int field1;
		public double field2;
		public boolean field3;

		public TestClass() {
		}

		public TestClass(int field1, double field2, boolean field3) {
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
}
