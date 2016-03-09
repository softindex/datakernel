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

package io.datakernel.jmx;

import javax.management.openmbean.OpenType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class Utils {

	private Utils() {}

	public static <T> List<T> filterNulls(List<T> src) {
		List<T> out = new ArrayList<>();
		for (T item : src) {
			if (item != null) {
				out.add(item);
			}
		}
		return out;
	}

	public static Map<String, OpenType<?>> createMapWithOneEntry(String key, OpenType<?> openType) {
		Map<String, OpenType<?>> map = new HashMap<>();
		map.put(key, openType);
		return map;
	}
}
