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

package io.datakernel.util;

import io.datakernel.util.ConcurrentStackPool.Supplier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class ConcurrentStackPoolTest {
	private interface StringSupplier {
		String get();
	}

	@Test
	public void testBase() {
		ConcurrentStackPool<StringSupplier> pool = new ConcurrentStackPool<>(new Supplier<StringSupplier>() {
			private int i = 1;

			@Override
			public StringSupplier get() {
				return new StringSupplier() {
					String value = i++ + "";

					@Override
					public String get() {
						return value;
					}
				};
			}
		});

		StringSupplier supplier1 = pool.get();
		StringSupplier supplier2 = pool.get();

		assertEquals("1", supplier1.get());
		assertEquals("2", supplier2.get());

		pool.put(supplier1);
		StringSupplier supplier3 = pool.get();
		assertEquals("1", supplier3.get());
	}

	@Test
	public void testDoesNotCreateInitialValueMethodIsNotOverrided() {
		ConcurrentStackPool<String> pool = new ConcurrentStackPool<>();

		String value = pool.get();

		assertNull(value);
	}
}