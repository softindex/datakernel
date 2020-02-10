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

package io.datakernel.config;

import io.datakernel.common.parse.ParseException;
import io.datakernel.common.parse.ParserFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.common.Preconditions.checkArgument;

public interface ConfigConverter<T> {
	T get(Config config, @Nullable T defaultValue);

	@NotNull
	T get(Config config);

	/**
	 * Applies given converter function to the converted value
	 *
	 * @param to   converter from T to V
	 * @param from converter from V to T
	 * @param <V>  return type
	 * @return converter that knows how to get V value from T value saved in config
	 */
	default <V> ConfigConverter<V> transform(ParserFunction<T, V> to, Function<V, T> from) {
		ConfigConverter<T> thisConverter = this;
		return new ConfigConverter<V>() {
			@Override
			public V get(Config config, @Nullable V defaultValue) {
				T value = thisConverter.get(config, defaultValue == null ? null : from.apply(defaultValue));
				try {
					return value != null ? to.parse(value) : null;
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@NotNull
			@Override
			public V get(Config config) {
				try {
					return to.parse(thisConverter.get(config));
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}
		};
	}

	default ConfigConverter<T> withConstraint(Predicate<T> predicate) {
		ConfigConverter<T> thisConverter = this;
		return new ConfigConverter<T>() {
			@Override
			public T get(Config config, T defaultValue) {
				T value = thisConverter.get(config, defaultValue);
				return checkArgument(value, predicate, () -> "Constraint violation: " + value);
			}

			@NotNull
			@Override
			public T get(Config config) {
				T value = thisConverter.get(config);
				return checkArgument(value, predicate, () -> "Constraint violation: " + value);
			}
		};
	}
}
