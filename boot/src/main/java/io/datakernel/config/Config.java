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

import io.datakernel.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;

/**
 * Represents properties in tree form and contains methods that allow to work
 * with config tree. Provides convenient static factory methods for
 * instantiating, persisting and modifying a {@code Config}. The {@code Config}
 * can be instantiated with a given {@link Properties} object, file or path to
 * file with properties.
 * <p>
 * The property consists of a key and a value. The tree path can represent
 * either a whole key or several chunks of some property's key. Key parts must
 * be separated using "." delimiter. The value of the property is stored in the
 * last node of the tree path, represented by a whole key.
 * <p>
 * In addition, there is a helpful {@link ConfigConverter} interface along with
 * {@link ConfigConverters} class, which provides a lot of handy converter
 * implementations.
 * <p>
 * For example, consider a config instance, which stores several properties:
 * <pre><code>
 * Config config = Config.create(); //create a config
 *
 * String key1 = "socket_addr";
 * String key2 = "is_available";
 * String key3 = "connections_count";
 *
 * config.set(property1Key, "250.200.100.50:10000");
 * config.set(property2Key, "true");
 * config.set(property3Key, "10");
 * </code></pre>
 * Next code snippet demonstrates the convenience of config converters:
 * <pre><code>
 * //instantiating converters (static import may be used instead)
 * ConfigConverter&lt;InetSocketAddress&gt; isac = ConfigConverters.ofInetSocketAddress();
 * ConfigConverter&lt;Boolean&gt; bc = ConfigConverters.ofBoolean();
 * ConfigConverter&lt;Integer&gt; ic= ConfigConverters.ofInteger();
 *
 * //will return an InetSocketAddress object (250.200.100.50:10000)
 * config.get(isac, property1Key);
 *
 * //will return Boolean true
 * config.get(bc, property2Key);
 *
 * //will return Integer 10
 * config.get(ic, property3Key);
 * </code></pre>
 *
 * @see ConfigConverter
 * @see ConfigConverters
 */
@SuppressWarnings("unchecked")
public final class Config {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final String ROOT = "";
	public static final char SEPARATOR = '.';

	private final Map<String, Config> children = new LinkedHashMap<>();

	private final Config parent;
	private String value;
	private String defaultValue;

	private boolean accessed;
	private boolean modified;

	private Config() {
		this.parent = null;
	}

	public static Config create() {
		return new Config();
	}

	private Config(Config parent) {
		this.parent = parent;
	}

	/**
	 * Creates a config with properties, stored in specified object.
	 *
	 * @param properties config properties
	 * @return config with given properties
	 */
	public static Config ofProperties(Properties properties) {
		Config root = new Config(null);
		for (String key : properties.stringPropertyNames()) {
			Config entry = root.ensureChild(key);
			entry.value = properties.getProperty(key);
		}
		return root;
	}

	/**
	 * Creates a config with properties contained in the file.
	 *
	 * @param propertiesFile	file with properties
	 * @return					config with properties from file
	 * @throws RuntimeException	if an input or output exception occures
	 */
	public static Config ofProperties(File propertiesFile) {
		final Properties properties = new Properties();
		try (InputStream fis = new FileInputStream(propertiesFile)) {
			properties.load(fis);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return ofProperties(properties);
	}

	/**
	 * Creates a config with properties from a file on the specified path.
	 *
	 * @param propertiesFile	path to properties file
	 * @return					config with properties from file
	 * @throws RuntimeException	if an input or output exception occured
	 */
	public static Config ofProperties(String propertiesFile) {
		return ofProperties(propertiesFile, false);
	}

	/**
	 * Creates a config with properties from a file on the specified
	 * filepath.
	 *
	 * @param propertiesFile	path to properties file
	 * @param optional
	 * @return					config with properties from file
	 * @throws RuntimeException	if an input or output exception occured
	 */
	public static Config ofProperties(String propertiesFile, boolean optional) {
		final File file = new File(propertiesFile);
		return ofProperties(file, optional);
	}

	/**
	 * Creates a config from a given file if file exists. In case of file
	 * absence returns blank config if optional is true or throws
	 * {@code RuntimeException} otherwise.
	 *
	 * @param file		file containing properties
	 * @param optional	defines behaviour if file doesn't exist
	 * @return			config with properties from file or blank config
	 * @throws RuntimeException	if an input or output exception occured
	 */
	public static Config ofProperties(File file, boolean optional) {
		if (!file.exists() && optional) {
			return new Config(null);
		}
		return ofProperties(file);
	}

	/**
	 * Creates a single config, consisting of specified configs.
	 *
	 * @param configs	set of configs to unite
	 * @return			single config
	 */
	public static Config union(Config... configs) {
		return doUnion(null, asList(configs));
	}

	/**
	 * Creates a single config object, consisting of configs, contained
	 * in specified collection.
	 *
	 * @param configs	collection of configs
	 * @return			single config
	 */
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
				if (result.value != null) {
					throw new IllegalStateException("Multiple values for " + config.getKey());
				}
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

	/**
	 * Saves config to the specified file using {@code UTF-8} charset
	 *
	 * @param file			the file to be opened for saving
	 * @throws IOException	if an I/O error occurs
	 */
	public void saveToPropertiesFile(File file) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))) {
			saveToPropertiesFile("", writer);
		}
	}

	synchronized public Config ensureChild(String path) {
		if (path.isEmpty())
			return this;
		Config result = this;
		for (String key : StringUtils.splitToList(SEPARATOR, path)) {
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

	/**
	 * Returns a config tree node which corresponds to the specified path.
	 *
	 * @param path	path in the config tree
	 * @return		config tree node
	 */
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

	/**
	 * Returns read-only map of current config's children.
	 *
	 * @return	unmodifiable map of config's child nodes
	 */
	synchronized public Map<String, Config> getChildren() {
		return Collections.unmodifiableMap(children);
	}

	/**
	 * Returns a tree path to this config. Returned path's chunks are separated
	 * by "."
	 *
	 * @return	key of config property
	 */
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

	/**
	 * Returns a value of the property and marks it as accessed.
	 *
	 * @return	value of the property
	 */
	synchronized public String get() {
		accessed = true;
		return value;
	}

	/**
	 * Assigns the default value of the property. Returns either value
	 * or default value of the property.
	 *
	 * @param defaultValue	default value of a property
	 * @return				value if it is not null; default value otherwise
	 * @throws IllegalArgumentException	in attempt to override default value
	 */
	synchronized public String get(String defaultValue) {
		this.defaultValue = defaultValue;
		String result = get();
		if (result == null) {
			logger.info("Using default value for '{}'", getKey());
			result = defaultValue;
		}
		return result;
	}

	/**
	 * Sets value of a property.
	 *
	 * @param value	value of a property
	 */
	synchronized public void set(String value) {
		modified = true;
		this.value = value;
	}

	/**
	 * Sets property's value on given path and marks it as modified.
	 * Creates non-existent nodes on the specified tree path.
	 *
	 * @param path	path to property's value in config tree
	 * @param value	value of config property
	 */
	synchronized public void set(String path, String value) {
		ensureChild(path).set(value);
	}

	/**
	 * Returns a value of tree node on specified path and converts it using
	 * converter. A {@link ConfigConverters} class has a lot of handy
	 * implementations of converters.
	 *
	 * @param converter	config converter
	 * @param path		path to property's value in config tree
	 * @param <T>		a type of property value
	 * @return			a value of property on the tree path
	 *
	 * @see ConfigConverter
	 * @see ConfigConverters
	 */
	synchronized public <T> T get(ConfigConverter<T> converter, String path) {
		checkNotNull(converter);
		return converter.get(ensureChild(path));
	}

	/**
	 * Acts like {@link #get(String)} and converts result using specified
	 * converter.
	 * <p>
	 * {@link ConfigConverters} class has a lot of handy
	 * implementations of converters.
	 *
	 * @param converter	config converter
	 * @param path		path to property's value in config tree
	 * @param <T>		a type of property value
	 * @return			a value of property on the tree path
	 *
	 * @see ConfigConverter
	 * @see ConfigConverters
	 */
	synchronized public <T> T get(ConfigConverter<T> converter, String path, T defaultValue) {
		checkNotNull(converter);
		return converter.get(ensureChild(path), defaultValue);
	}

	/**
	 * Checks if there is a value assigned to a node. Creates non-existent nodes
	 * of the specified tree path.
	 *
	 * @param path	path to required {@code Config} node
	 * @return		true if value is not null; false otherwise
	 */
	synchronized public boolean hasValue(String path) {
		Config child = ensureChild(path);
		child.accessed = true;
		return child.value != null;
	}

	/**
	 * Checks for existence of subsequent parts of property's key.
	 * <p>
	 * For the given {@link Config config} object:
	 * <pre><code>
	 * composite.property.example.integer=0;
	 * composite.property.example.character=c;
	 * composite.property.value=0;
	 * </code></pre>
	 * The results of invocation will be the following:
	 * <pre><code>
	 * config.hasSection("composite.property"); //true
	 * config.hasSection("composite.property.example"); //true
	 * config.hasSection("composite.property.value"); //false
	 * </code></pre>
	 * The result of the last method invocation is false because reached leaf
	 * node has assigned value.
	 *
	 * @param path	path to required {@code Config} node
	 * @return		true if node on the specified path has children with null
	 * 				value; false otherwise
	 */
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

}
