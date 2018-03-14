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

import io.datakernel.annotation.Nullable;

public abstract class ComplexConfigConverter<T> implements ConfigConverter<T> {
	private final T defaultValue;

	protected ComplexConfigConverter(T defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	@Nullable
	public final T get(Config config) {
		return provide(config, defaultValue);
	}

	@Override
	@Nullable
	public final T get(Config config, @Nullable T defaultValue) {
		boolean isNull = defaultValue == null;
		if (defaultValue == null) {
			defaultValue = this.defaultValue;
		}
		T result = provide(config, defaultValue);
		if (isNull && config.isEmpty()) {
			return null;
		}
		return result;
	}

	protected abstract T provide(Config config, T defaultValue);
}
