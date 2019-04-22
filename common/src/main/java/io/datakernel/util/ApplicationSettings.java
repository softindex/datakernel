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

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public final class ApplicationSettings {
	private ApplicationSettings() {
		throw new AssertionError();
	}

	@Nullable
	@Contract("_, _, !null -> !null")
	public static String getString(Class<?> type, String name, @Nullable String defValue) {
		String property;
		property = System.getProperty(type.getName() + "." + name);
		if (property != null) return property;
		property = System.getProperty(type.getSimpleName() + "." + name);
		if (property != null) return property;
		return defValue;
	}

	public static int getInt(Class<?> type, String name, int defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Integer.parseInt(property);
		}
		return defValue;
	}

	public static long getLong(Class<?> type, String name, long defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Long.parseLong(property);
		}
		return defValue;
	}

	public static boolean getBoolean(Class<?> type, String name, boolean defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Boolean.parseBoolean(property);
		}
		return defValue;
	}

	public static double getDouble(Class<?> type, String name, double defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return Double.parseDouble(property);
		}
		return defValue;
	}

	@Nullable
	@Contract("_, _, !null -> !null")
	public static Duration getDuration(Class<?> type, String name, @Nullable Duration defValue) {
		String property = getString(type, name, null);
		if (property != null) {
			return StringFormatUtils.parseDuration(property);
		}
		return defValue;
	}
}
