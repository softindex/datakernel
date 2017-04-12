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

public abstract class AbstractConfigConverter<T> implements ConfigConverter<T> {
	@Override
	public final T get(Config config) {
		String value = config.get(Config.THIS);
		return value == null ? null : fromString(value);
	}

	@Override
	public final T get(Config config, T defaultValue) {
		String defaultString = defaultValue == null ? null : toString(defaultValue);
		return fromString(config.get(Config.THIS, defaultString));
	}

	protected abstract T fromString(String value);

	protected abstract String toString(T defaultValue);
}
