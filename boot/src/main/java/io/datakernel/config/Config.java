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

import io.datakernel.util.Splitter;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import static io.datakernel.util.Preconditions.*;
import static java.util.Arrays.asList;

public final class Config {
	private static final Splitter SPLITTER = Splitter.on('.');

	private final Map<String, Config> children = new LinkedHashMap<>();

	private final Config parent;
	private String value;
	private String defaultValue;

	private boolean accessed;
	private boolean modified;

	public Config() {
		this.parent = null;
	}

	private Config(Config parent) {
		this.parent = parent;
	}

	public static Config ofProperties(Properties properties) {
		Config root = new Config(null);
		for (String key : properties.stringPropertyNames()) {
			Config entry = root.ensureChild(key);
			entry.value = properties.getProperty(key);
		}
		return root;
	}

	public static Config ofProperties(File propertiesFile) {
		final Properties properties = new Properties();
		try (InputStream fis = new FileInputStream(propertiesFile)) {
			properties.load(fis);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return ofProperties(properties);
	}

	public static Config ofProperties(String propertiesFile) {
		return ofProperties(propertiesFile, false);
	}

	public static Config ofProperties(String propertiesFile, boolean optional) {
		final File file = new File(propertiesFile);
		if (!file.exists() && optional)
			return new Config(null);
		return ofProperties(file);
	}

	public static Config union(Config... configs) {
		return doUnion(null, asList(configs));
	}

	public static Config union(Collection<Config> configs) {
		if (configs.size() == 1)
			return configs.iterator().next();
		return doUnion(null, configs);
	}

	private static Config doUnion(Config parent, Collection<Config> configs) {
		Config result = new Config(parent);
		Map<String, List<Config>> childrenList = new LinkedHashMap<>();

		for (Config config : configs) {
			if (config.value != null) {
				if (result.value != null)
					throw new IllegalStateException("Multiple values for " + config.getKey());
				result.value = config.value;
			}
			for (String key : config.children.keySet()) {
				Config child = config.children.get(key);
				List<Config> list = childrenList.get(key);
				if (list == null) {
					list = new ArrayList<>();
					childrenList.put(key, list);
				}
				list.add(child);
			}
		}

		for (String key : childrenList.keySet()) {
			List<Config> childConfigs = childrenList.get(key);
			Config joined = doUnion(result, childConfigs);
			result.children.put(key, joined);
		}

		return result;
	}

	private static String propertiesFileEncode(String string, boolean escapeKey) {
		StringBuilder sb = new StringBuilder(string.length() * 2);

		for (int i = 0; i < string.length(); i++) {
			char c = string.charAt(i);
			if ((c > 61) && (c < 127)) {
				if (c == '\\') {
					sb.append('\\');
					sb.append('\\');
					continue;
				}
				sb.append(c);
				continue;
			}
			switch (c) {
				case ' ':
					if (i == 0 || escapeKey)
						sb.append('\\');
					sb.append(' ');
					break;
				case '\t':
					sb.append('\\');
					sb.append('t');
					break;
				case '\n':
					sb.append('\\');
					sb.append('n');
					break;
				case '\r':
					sb.append('\\');
					sb.append('r');
					break;
				case '\f':
					sb.append('\\');
					sb.append('f');
					break;
				case '=':
				case ':':
				case '#':
				case '!':
					sb.append('\\');
					sb.append(c);
					break;
				default:
					sb.append(c);
			}
		}
		return sb.toString();
	}

	synchronized private boolean saveToPropertiesFile(String prefix, Writer writer) throws IOException {
		boolean rootLevel = prefix.isEmpty();
		StringBuilder sb = new StringBuilder();
		if (value != null || defaultValue != null) {
			if (!accessed) {
				assert defaultValue == null;
				sb.append("# Unused: ");
				sb.append(propertiesFileEncode(prefix, true));
				sb.append(" = ");
				sb.append(propertiesFileEncode(value, false));
			} else {
				if (value != null && !value.equals(defaultValue)) {
					sb.append(propertiesFileEncode(prefix, true));
					sb.append(" = ");
					sb.append(propertiesFileEncode(value, false));
				} else { // defaultValue != null
					sb.append("#");
					sb.append(propertiesFileEncode(prefix, true));
					sb.append(" = ");
					if (defaultValue != null) {
						sb.append(propertiesFileEncode(defaultValue, false));
					}
				}
			}
		}
		boolean saved = false;
		String line = sb.toString();
		if (!line.isEmpty()) {
			writer.write(line + '\n');
			saved = true;
		}
		for (String key : children.keySet()) {
			Config child = children.get(key);
			boolean savedByChild = child.saveToPropertiesFile(rootLevel ? key : (prefix + "." + key), writer);
			if (rootLevel && savedByChild) {
				writer.write('\n');
			}
			saved |= savedByChild;
		}
		return saved;
	}

	public void saveToPropertiesFile(File file) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))) {
			saveToPropertiesFile("", writer);
		}
	}

	public void saveToPropertiesFile(String file) throws IOException {
		saveToPropertiesFile(new File(file));
	}

	private Config ensureChild(String path) {
		checkArgument(!path.isEmpty(), "Path must not be empty");
		Config result = this;
		for (String key : SPLITTER.split(path)) {
			checkState(!key.isEmpty(), "Child path must not be empty: %s", path);
			Config child = result.children.get(key);
			if (child == null) {
				child = new Config(result);
				result.children.put(key, child);
			}
			result = child;
		}
		return result;
	}

	synchronized public Config getChild(String path) {
		final Config config = ensureChild(path);
		config.accessed = true;
		return config;
	}

	synchronized public boolean isAccessed() {
		return accessed;
	}

	synchronized public boolean isModified() {
		return modified;
	}

	synchronized public Map<String, Config> getChildren() {
		return Collections.unmodifiableMap(children);
	}

	synchronized public String getKey() {
		if (parent == null)
			return "";
		for (String childKey : parent.children.keySet()) {
			Config child = parent.children.get(childKey);
			if (child == this) {
				String childRootKey = parent.getKey();
				return childRootKey.isEmpty() ? childKey : childRootKey + "." + childKey;
			}
		}
		throw new IllegalStateException();
	}

	synchronized public String get() {
		accessed = true;
		return value;
	}

	synchronized public String get(String defaultValue) {
		if (this.defaultValue != null) {
			if (!this.defaultValue.equals(defaultValue)) {
				throw new IllegalArgumentException("Key '" + getKey() + "': Previous default value '" + this.defaultValue + "' differs from new default value '"
						+ defaultValue + "'");
			}
		} else {
			this.defaultValue = defaultValue;
		}
		String result = get();
		return result != null ? result : defaultValue;
	}

	synchronized public void set(String value) {
		modified = true;
		this.value = value;
	}

	synchronized public void set(String path, String value) {
		ensureChild(path).set(value);
	}

	synchronized public <T> T get(ConfigConverter<T> converter) {
		checkNotNull(converter);
		return converter.get(this);
	}

	synchronized public <T> T get(ConfigConverter<T> converter, String path) {
		checkNotNull(converter);
		return ensureChild(path).get(converter);
	}

	synchronized public <T> T getDefault(ConfigConverter<T> converter, T defaultValue) {
		checkNotNull(converter);
		checkNotNull(defaultValue);
		return converter.get(this, defaultValue);
	}

	synchronized public <T> T get(ConfigConverter<T> converter, String path, T defaultValue) {
		checkNotNull(converter);
		checkNotNull(defaultValue);
		return ensureChild(path).getDefault(converter, defaultValue);
	}

	synchronized public <T> void set(ConfigConverter<T> converter, T value) {
		checkNotNull(converter);
		checkNotNull(value);
		converter.set(this, value);
	}

	synchronized public <T> void set(ConfigConverter<T> converter, String path, T value) {
		checkNotNull(converter);
		checkNotNull(value);
		ensureChild(path).set(converter, value);
	}

	synchronized public boolean hasValue(String path) {
		Config child = ensureChild(path);
		child.accessed = true;
		return child.value != null;
	}

	synchronized public boolean hasSection(String path) {
		Config child = ensureChild(path);
		child.accessed = true;
		return child.children.size() > 0 && child.value == null;
	}

	@Override
	public String toString() {
		return getKey();
	}

	@Override
	public synchronized boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Config config = (Config) o;
		return Objects.equals(this.children, config.children) &&
				Objects.equals(this.value, config.value);
	}

	public synchronized boolean contains(Config config) {
		if (this == config)
			return true;
		if (!Objects.equals(this.value, config.value))
			return false;
		for (Entry<String, Config> entry : config.children.entrySet()) {
			Config childrenConfig = this.children.get(entry.getKey());
			if (childrenConfig == null || !childrenConfig.contains(entry.getValue()))
				return false;
		}
		return true;
	}

	/**
	 * Fully clone of source Config
	 *
	 * @param config source config
	 * @return fully clone of source Config
	 */
	public static Config clone(Config config) {
		return clone(config, null);
	}

	private static Config clone(Config config, Config parent) {
		Config clone = new Config(parent);
		for (Entry<String, Config> entry : config.children.entrySet()) {
			Config clonedConfig = clone(entry.getValue(), clone);
			clone.children.put(entry.getKey(), clonedConfig);
		}
		clone.accessed = config.accessed;
		clone.defaultValue = config.defaultValue;
		clone.value = config.value;
		return clone;
	}

	public synchronized void removeUnused() {
		for (Entry<String, Config> entry : new ArrayList<>(children.entrySet())) {
			final Config value = entry.getValue();
			if (value.children.isEmpty()) {
				if (!value.accessed)
					children.remove(entry.getKey());
			} else {
				value.removeUnused();
			}
		}
	}

	public synchronized boolean update(Config config) {
		checkNotNull(config);
		boolean updated = false;
		if (!Objects.equals(this.value, config.value)) {
			this.value = config.value;
			updated = true;
		}

		for (Entry<String, Config> entry : config.children.entrySet()) {
			String key = entry.getKey();
			Config value = entry.getValue();
			if (!this.children.containsKey(key)) {
				this.children.put(key, clone(value));
				updated = true;
			} else {
				if (this.children.get(key).update(value)) {
					updated = true;
				}
			}
		}
		return updated;
	}

	public synchronized Config diff(Config config) {
		checkNotNull(config);

		Config diff = new Config();
		if (!Objects.equals(this.value, config.value)) {
			diff.value = config.value;
		}

		for (Entry<String, Config> entry : config.children.entrySet()) {
			String key = entry.getKey();
			Config value = entry.getValue();
			if (!this.children.containsKey(key)) {
				diff.children.put(key, clone(value));
			} else {
				Config subDiff = this.children.get(key).diff(value);
				if (subDiff != null)
					diff.children.put(key, subDiff);
			}
		}
		if (diff.value == null && diff.children.isEmpty())
			return null;
		return diff;
	}
}
