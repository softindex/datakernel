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

import java.time.Duration;
import java.util.function.Function;

public final class ApplicationSettings {
	private ApplicationSettings() {
		throw new AssertionError();
	}

	public static String getString(Class<?> type, String name, String defValue) {
		String property;
		property = System.getProperty(type.getName() + "." + name);
		if (property != null) return property;
		property = System.getProperty(type.getSimpleName() + "." + name);
		if (property != null) return property;
		return defValue;
	}

	public static <T> T get(Function<String, T> parser, Class<?> type, String name, T defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return parser.apply(property);
		}
		return defValue;
	}

	public static int getInt(Class<?> type, String name, int defValue) {
		return get(Integer::parseInt, type, name, defValue);
	}

	public static long getLong(Class<?> type, String name, long defValue) {
		return get(Long::parseLong, type, name, defValue);
	}

	public static boolean getBoolean(Class<?> type, String name, boolean defValue) {
		return get(Boolean::parseBoolean, type, name, defValue);
	}

	public static double getDouble(Class<?> type, String name, double defValue) {
		return get(Double::parseDouble, type, name, defValue);
	}

	public static Duration getDuration(Class<?> type, String name, Duration defValue) {
		return get(StringFormatUtils::parseDuration, type, name, defValue);
	}

	public static MemSize getMemSize(Class<?> type, String name, MemSize defValue) {
		return get(MemSize::valueOf, type, name, defValue);
	}
}
