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

package io.datakernel.common;

import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static io.datakernel.common.Preconditions.checkState;
import static java.util.Collections.emptyMap;

public final class ApplicationSettings {
	private static final Map<Class<?>, Map<String, Object>> customSettings = new HashMap<>();

	private static Properties properties = System.getProperties();
	private static volatile boolean firstLookupDone = false;

	public static void useProperties(Properties properties) {
		ensureNotLookedUp();
		ApplicationSettings.properties = properties;
	}

	public static void set(Class<?> type, String name, Object value) {
		ensureNotLookedUp();
		customSettings.computeIfAbsent(type, $ -> new HashMap<>()).put(name, value);
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(Function<String, T> parser, Class<?> type, String name, T defValue) {
		firstLookupDone = true;
		T customSetting = (T) customSettings.getOrDefault(type, emptyMap()).get(name);
		if (customSetting != null) {
			return customSetting;
		}
		String property = getProperty(type, name);
		if (property != null) {
			return parser.apply(property);
		}
		return defValue;
	}

	public static String getString(Class<?> type, String name, String defValue) {
		return get(Function.identity(), type, name, defValue);
	}

	public static int getInt(Class<?> type, String name, int defValue) {
		return get(Integer::parseInt, type, name, defValue);
	}

	public static long getLong(Class<?> type, String name, long defValue) {
		return get(Long::parseLong, type, name, defValue);
	}

	public static boolean getBoolean(Class<?> type, String name, boolean defValue) {
		return get(s -> s.trim().isEmpty() || Boolean.parseBoolean(s), type, name, defValue);
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

	public static Path getPath(Class<?> type, String name, Path defValue){
		return get(Paths::get, type, name, defValue);
	}

	@Nullable
	private static String getProperty(Class<?> type, String name) {
		String property;
		property = properties.getProperty(type.getName() + "." + name);
		if (property != null) return property;
		property = properties.getProperty(type.getSimpleName() + "." + name);
		return property;
	}

	private static void ensureNotLookedUp() {
		checkState(!firstLookupDone, () -> "Attempting to update application settings after some of them have been retrieved\n" +
				"All updates should happen prior to any constant initialization via ApplicationSettings, " +
				"preferably in static initialization block of 'main' class");
	}
}
