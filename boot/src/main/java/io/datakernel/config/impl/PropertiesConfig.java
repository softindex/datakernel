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

package io.datakernel.config.impl;

import io.datakernel.config.AbstractConfig;
import io.datakernel.config.Config;
import io.datakernel.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.datakernel.config.Configs.EMPTY_CONFIG;
import static io.datakernel.util.Preconditions.checkArgument;

public final class PropertiesConfig extends AbstractConfig {
	private static final Logger logger = LoggerFactory.getLogger(PropertiesConfig.class);

	private static class TrieNode {
		final Map<String, TrieNode> nodes = new TreeMap<>();

		static TrieNode ofProperties(Properties properties) {
			TrieNode node = new TrieNode();
			for (String propertyName : properties.stringPropertyNames()) {
				node.add(StringUtils.splitToList(DELIMITER, propertyName).stream()
						.filter(Objects::nonNull)
						.filter(s -> !s.isEmpty())
						.map(String::trim)
						.iterator());
			}
			return node;
		}

		void add(Iterator<String> path) {
			if (path.hasNext()) {
				String next = path.next();
				TrieNode node = nodes.get(next);
				if (node == null) {
					node = new TrieNode();
					nodes.put(next, node);
				}
				node.add(path);
			}
		}
	}

	private final String rootPath;
	private final Properties properties;
	private final TrieNode trie;

	// creators
	private PropertiesConfig(String rootPath, Properties properties, TrieNode trie) {
		this.rootPath = rootPath;
		this.properties = properties;
		this.trie = trie;
	}

	public static PropertiesConfig ofProperties(Properties properties) {
		return new PropertiesConfig("", properties, TrieNode.ofProperties(properties));
	}

	public static PropertiesConfig ofProperties(String fileName) {
		return ofProperties(fileName, false);
	}

	public static PropertiesConfig ofProperties(String fileName, boolean optional) {
		return ofProperties(Paths.get(fileName), optional);
	}

	public static PropertiesConfig ofProperties(Path file, boolean optional) {
		Properties props = new Properties();
		try (InputStream is = Files.newInputStream(file)) {
			props.load(is);
		} catch (IOException e) {
			if (optional) {
				logger.warn("Can't load properties file: {}", file);
			} else {
				throw new IllegalArgumentException("Failed to load required properties: " + file.toString(), e);
			}
		}
		return ofProperties(props);
	}

	private PropertiesConfig withRootPathAndKey(String rootPath, String path) {
		String newRootPath = rootPath.isEmpty() || path.isEmpty() ? rootPath + path : rootPath + "." + path;
		return new PropertiesConfig(newRootPath, properties, trie.nodes.get(path));
	}

	// api
	@Override
	protected String doGet() {
		String value = properties.getProperty(rootPath);
		if (value == null) {
			throw new NoSuchElementException(rootPath);
		}
		return value;
	}

	@Override
	protected String doGet(String defaultValue) {
		String property = properties.getProperty(rootPath);
		if (property == null) {
			logger.warn("using default config for {}", rootPath);
			return defaultValue;
		}
		return property;
	}

	@Override
	public boolean hasValue() {
		return properties.getProperty(rootPath) != null;
	}

	@Override
	protected boolean doHasChild(String path) {
		checkArgument(!path.contains("."));
		String propertyName = rootPath.isEmpty() ? path : rootPath + "." + path;
		Set<String> names = properties.stringPropertyNames();
		for (String name : names) {
			if (name.startsWith(propertyName))
				return true;
		}
		return false;
	}

	@Override
	protected Config doGetChild(String key) {
		checkArgument(!key.contains("."));
		if (!doHasChild(key)) {
			return EMPTY_CONFIG;
		}
		return withRootPathAndKey(rootPath, key);
	}

	@Override
	public Set<String> getChildren() {
		if (trie == null) {
			return Collections.emptySet();
		}
		return trie.nodes.keySet();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PropertiesConfig that = (PropertiesConfig) o;

		return this.rootPath.equals(that.rootPath) && this.properties.equals(that.properties);
	}

	@Override
	public int hashCode() {
		return 31 * rootPath.hashCode() + properties.hashCode();
	}

	@Override
	public String toString() {
		return "PropertiesConfig[" + properties + "]";
	}
}
