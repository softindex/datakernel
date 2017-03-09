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

/**
 * Such converters are able to make bilateral conversions between data type and
 * its string representation.
 * <p>
 * There are a lot of converters implemented in {@link ConfigConverters} class.
 *
 * @param <T> data type for conversion
 *
 * @see ConfigConverters
 */
public abstract class AbstractConfigConverter<T> implements ConfigConverter<T> {

	public abstract T fromString(String string);

	public T fromEmptyString() {
		return null;
	}

	public abstract String toString(T item);

	/**
	 * Returns a value of a property, represented by a given config.
	 *
	 * @param config		a config instance which represents a property
	 * @return				value of the property
	 */
	@Override
	public final T get(Config config) {
		checkState(config.getChildren().isEmpty());
		String string = config.get();
		checkNotNull(string, "Config %s not found", config);
		string = string.trim();
		return string.isEmpty() ? fromEmptyString() : fromString(string);
	}

	/**
	 * Returns a value of a property, represented by a given config.
	 * Assigns the default value of the property.
	 *
	 * @param config		a config instance which represents a property
	 * @param defaultValue	default value of the property
	 * @return				value of the property if it exists or null otherwise
	 */
	@Override
	public final T get(Config config, T defaultValue) {
		checkState(config.getChildren().isEmpty());
		String defaultString = defaultValue == null ? "" : toString(defaultValue);
		String string = config.get(defaultString);
		checkNotNull(string, "Config %s not found", config);
		string = string.trim();
		T result = string.isEmpty() ? fromEmptyString() : fromString(string);
		config.set(result == null ? "" : toString(result));
		return result;
	}
}
