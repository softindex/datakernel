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

import org.junit.Test;

import java.util.function.Supplier;

import static io.datakernel.bytebuf.ByteBufTest.initByteBufPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class ConcurrentStackPoolTest {
	static {
		initByteBufPool();
	}

	@Test
	public void testBase() {
		ConcurrentStackPool<Supplier<String>> pool = new ConcurrentStackPool<>(new Supplier<Supplier<String>>() {
			private int i = 1;

			@Override
			public Supplier<String> get() {
				return new Supplier<String>() {
					String value = i++ + "";

					@Override
					public String get() {
						return value;
					}
				};
			}
		});

		Supplier<String> supplier1 = pool.get();
		Supplier<String> supplier2 = pool.get();

		assertEquals("1", supplier1.get());
		assertEquals("2", supplier2.get());

		pool.put(supplier1);
		Supplier<String> supplier3 = pool.get();
		assertEquals("1", supplier3.get());
	}

	@Test
	public void testDoesNotCreateInitialValueMethodIsNotOverrided() {
		ConcurrentStackPool<String> pool = new ConcurrentStackPool<>();

		String value = pool.get();

		assertNull(value);
	}
}
