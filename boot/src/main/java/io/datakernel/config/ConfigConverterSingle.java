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

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public abstract class ConfigConverterSingle<T> implements ConfigConverter<T> {

	protected abstract T fromString(String string);

	protected abstract String toString(T item);

	@Override
	public final T get(Config config) {
		checkState(config.getChildren().isEmpty());
		String string = config.get();
		checkNotNull(string, "Config %s not found", config);
		return fromString(string);
	}

	@Override
	public final T get(Config config, T defaultValue) {
		checkState(config.getChildren().isEmpty());
		String defaultString = toString(defaultValue);
		String string = config.get(defaultString);
		checkNotNull(string);
		string = string.trim();
		T result = fromString(string);
		checkNotNull(result);
		config.set(toString(result));
		return result;
	}

	@Override
	public final void set(Config config, T item) {
		checkState(config.getChildren().isEmpty());
		config.set(toString(item));
	}
}
