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

import com.google.common.reflect.TypeToken;

import java.util.Map;

public interface Config {
	Config getChild(String path);

	Map<String, Config> getChildren();

	<T> T get(TypeToken<T> type);

	<T> T get(Class<T> type);

	<T> T get(TypeToken<T> type, T defaultValue);

	<T> T get(Class<T> type, T defaultValue);

	<T> T get(String path, TypeToken<T> type);

	<T> T get(String path, Class<T> type);

	<T> T get(String path, TypeToken<T> type, T defaultValue);

	<T> T get(String path, Class<T> type, T defaultValue);
}
