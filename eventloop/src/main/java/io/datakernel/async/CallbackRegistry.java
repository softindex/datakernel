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

import java.util.concurrent.ConcurrentHashMap;

public final class CallbackRegistry {
	private static final ConcurrentHashMap<Object, Long> REGISTRY = new ConcurrentHashMap<>();

	public static void register(Object callback) {
		assert REGISTRY.put(callback, System.currentTimeMillis()) == null : "Callback " + callback + " has already been registered";
	}

	public static void complete(Object callback) {
		assert REGISTRY.remove(callback) != null : "Callback " + callback + " has already been completed";
	}
}
