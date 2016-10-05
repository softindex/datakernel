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

package io.datakernel.async;

import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.datakernel.jmx.MBeanFormat.formatDuration;

public final class CallbackRegistry {
	private static final ConcurrentHashMap<Object, Long> REGISTRY = new ConcurrentHashMap<>();

	public static void register(Object callback) {
		assert REGISTRY.put(callback, System.currentTimeMillis()) == null : "Callback " + callback + " has already been registered";
	}

	public static void complete(Object callback) {
		assert REGISTRY.remove(callback) != null : "Callback " + callback + " has already been completed";
	}

	public static final class CallbackRegistryStats implements ConcurrentJmxMBean {

		@JmxAttribute
		public List<String> getCurrentCallbacks() {
			List<Map.Entry<Object, Long>> entries = new ArrayList<>(REGISTRY.entrySet());
			Collections.sort(entries, new Comparator<Map.Entry<Object, Long>>() {
				@Override
				public int compare(Map.Entry<Object, Long> o1, Map.Entry<Object, Long> o2) {
					return o1.getValue().compareTo(o2.getValue());
				}
			});

			List<String> lines = new ArrayList<>();
			lines.add("Duration       Callback");
			long currentTime = System.currentTimeMillis();
			for (Map.Entry<Object, Long> entry : entries) {
				String duration = formatDuration(currentTime - entry.getValue());
				Object callback = entry.getKey();
				String className = callback.getClass().getName();
				String callbackString = callback.toString();
				lines.add(String.format("%s   %s: %s", duration, className, callbackString));
			}

			return lines;
		}
	}
}
