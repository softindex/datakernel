/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.annotation.Nullable;

import java.util.function.Supplier;

public final class ConcurrentStackPool<V> {
	private final ConcurrentStack<V> stack = new ConcurrentStack<>();
	@Nullable
	private final Supplier<V> valueSupplier;

	public ConcurrentStackPool() {
		this(null);
	}

	public ConcurrentStackPool(@Nullable Supplier<V> valueSupplier) {
		this.valueSupplier = valueSupplier;
	}

	@Nullable
	public final V get() {
		V element = stack.pop();
		if (element == null && valueSupplier != null) {
			element = valueSupplier.get();
		}
		return element;
	}

	public final void put(V element) {
		stack.push(element);
	}
}
