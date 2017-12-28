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

public abstract class AbstractConfig implements Config {

	@Override
	public final String get(String path) {
		if (path.equals(THIS)) {
			return trimIfNotNull(doGet());
		} else {
			int dot = path.indexOf(DELIMITER);
			if (dot == -1) {
				return getChild(path).get(THIS);
			} else {
				String key = path.substring(0, dot);
				String relativePath = path.substring(dot + 1, path.length());
				return getChild(key).get(relativePath);
			}
		}
	}

	@Override
	public final String get(String path, String defaultValue) {
		if (path.equals(THIS)) {
			return trimIfNotNull(doGet(defaultValue));
		} else {
			int dot = path.indexOf(DELIMITER);
			if (dot == -1) {
				return getChild(path).get(THIS, defaultValue);
			} else {
				String key = path.substring(0, dot);
				String newPath = path.substring(dot + 1, path.length());
				return getChild(key).get(newPath, defaultValue);
			}
		}
	}

	@Override
	public final <T> T get(ConfigConverter<T> converter, String path) {
		return converter.get(getChild(path));
	}

	@Override
	public final <T> T get(ConfigConverter<T> converter, String path, T defaultValue) {
		return converter.get(getChild(path), defaultValue);
	}

	protected abstract String doGet();

	protected abstract String doGet(String defaultString);

	@Override
	public final Config getChild(String path) {
		if (path.equals(THIS)) {
			return this;
		}
		int dot = path.indexOf(DELIMITER);
		if (dot == -1) {
			return doGetChild(path);
		} else {
			String childName = path.substring(0, dot);
			String relativePath = path.substring(dot + 1, path.length());
			return doGetChild(childName).getChild(relativePath);
		}
	}

	protected abstract Config doGetChild(String key);

	@Override
	public final boolean hasChild(String path) {
		if (path.equals(THIS)) {
			return false;
		}
		int dot = path.indexOf(DELIMITER);
		if (dot == -1) {
			return doHasChild(path);
		} else {
			String childName = path.substring(0, dot);
			String relativePath = path.substring(dot + 1, path.length());
			return getChild(childName).hasChild(relativePath);
		}
	}

	protected abstract boolean doHasChild(String key);

	private String trimIfNotNull(String value) {
		if (value != null) {
			return value.trim();
		}
		return null;
	}
}
