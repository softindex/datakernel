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

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.launcher.OnStart;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.ConfigConverters.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
	public void testClassPathConfig() {
		Config config = Config.ofClassPathProperties("test.properties");
		assertNotNull(config);
	}

	@Test
	public void testClassPathConfigWithRoots() {
		Config config = Config.ofClassPathProperties("/test.properties");
		assertNotNull(config);
		config = Config.ofClassPathProperties("test.properties/");
		assertNotNull(config);
		config = Config.ofClassPathProperties("/test.properties/");
		assertNotNull(config);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPathConfig() {
		Config config = Config.ofProperties("test.properties");
		assertNotNull(config);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testClassPathNotFoundProperties() {
		Config.ofClassPathProperties("notFound.properties");
	}

	@Test
	public void testClassPathNotFoundPropertiesOptional() {
		Config.ofClassPathProperties("notFound.properties", true);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPathNameNotFoundProperties() {
		Config.ofProperties("notFound.properties");
	}

	@Test
	public void testPathNameNotFoundPropertiesOptional() {
		Config.ofProperties("notFound.properties", true);
	}

	@Test
	public void testConfigs() {
		Properties properties1 = new Properties();
		properties1.put("port", "1234");
		properties1.put("msg", "Test phrase");
		properties1.put("innerClass.field1", "2");
		properties1.put("innerClass.field2", "3.5");

		Properties properties2 = new Properties();
		properties2.put("workers", "4");
		properties2.put("innerClass.field3", "true");

		ConfigConverter<TestClass> configConverter = new ConfigConverter<TestClass>() {
			@NotNull
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

		CompletableFuture<Void> onStart = new CompletableFuture<>();

		Injector injector = Injector.of(
				new AbstractModule() {
					@Provides
					@OnStart
					CompletionStage<Void> onStart() {
						return onStart;
					}

					@Provides
					Config config() {
						return Config.create()
								.overrideWith(Config.ofProperties(properties1))
								.overrideWith(Config.ofProperties(properties2))
								.overrideWith(Config.ofProperties("not-existing.properties", true));
					}
				},
				ConfigModule.create()
						.withEffectiveConfigLogger()
		);

		Config config = injector.getInstance(Config.class);

		assertEquals(1234, (int) config.get(ofInteger(), "port"));
		assertEquals("Test phrase", config.get("msg"));
		assertEquals(new TestClass(2, 3.5, true), config.get(configConverter, "innerClass"));

		onStart.complete(null);
	}
}
