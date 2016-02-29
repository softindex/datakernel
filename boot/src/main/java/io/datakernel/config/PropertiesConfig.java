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
import io.datakernel.service.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public final class PropertiesConfig implements BlockingService, Config {
	private static final Logger logger = LoggerFactory.getLogger(PropertiesConfig.class);

	private ConfigNode root;
	private Path saveConfigPath;

	// creators
	private PropertiesConfig(ConfigNode root) {
		this.root = root;
	}

	public static PropertiesConfig ofProperties(List<Properties> properties) {
		return new PropertiesConfig(propertiesToConfig(properties)
				.registerConverter(Integer.class, ConfigConverters.ofInteger())
				.registerConverter(Long.class, ConfigConverters.ofLong())
				.registerConverter(Double.class, ConfigConverters.ofDouble())
				.registerConverter(String.class, ConfigConverters.ofString())
				.registerConverter(Boolean.class, ConfigConverters.ofBoolean()));
	}

	public static PropertiesConfig ofProperties(Properties... properties) {
		return ofProperties(Arrays.asList(properties));
	}

	public static PropertiesConfig ofProperties(Path... paths) {
		return ofProperties(loadProperties(Arrays.asList(paths)));
	}

	public static PropertiesConfig ofProperties(String... pathNames) {
		Path[] paths = new Path[pathNames.length];
		for (int i = 0; i < pathNames.length; i++) {
			paths[i] = Paths.get(pathNames[i]);
		}
		return ofProperties(paths);
	}

	public PropertiesConfig saveConfig(Path path) {
		this.saveConfigPath = path;
		return this;
	}

	public <T> PropertiesConfig registerConverter(Class<T> type, ConfigConverter<T> converter) {
		root.registerConverter(type, converter);
		return this;
	}

	public <T> PropertiesConfig registerConverter(TypeToken<T> type, ConfigConverter<T> converter) {
		root.registerConverter(type, converter);
		return this;
	}

	// blocking service
	@Override
	public void start() throws Exception {
		if (saveConfigPath != null) {
			root.saveToPropertiesFile(saveConfigPath);
		}
	}

	@Override
	public void stop() throws Exception {
		// empty
	}

	// config api
	@Override
	public Config getChild(String path) {
		return root.getChild(path);
	}

	@Override
	public Map<String, Config> getChildren() {
		return root.getChildren();
	}

	@Override
	public <T> T get(Class<T> type) {
		return root.get(type);
	}

	@Override
	public <T> T get(TypeToken<T> type) {
		return root.get(type);
	}

	@Override
	public <T> T get(Class<T> type, T defaultValue) {
		return root.get(type, defaultValue);
	}

	@Override
	public <T> T get(TypeToken<T> type, T defaultValue) {
		return root.get(type, defaultValue);
	}

	@Override
	public <T> T get(String path, Class<T> type) {
		return root.get(path, type);
	}

	@Override
	public <T> T get(String path, TypeToken<T> type) {
		return root.get(path, type);
	}

	@Override
	public <T> T get(String path, Class<T> type, T defaultValue) {
		return root.get(path, type, defaultValue);
	}

	@Override
	public <T> T get(String path, TypeToken<T> type, T defaultValue) {
		return root.get(path, type, defaultValue);
	}

	// utils
	private static ConfigNode propertiesToConfig(List<Properties> properties) {
		List<ConfigNode> configs = new ArrayList<>();
		for (Properties property : properties) {
			ConfigNode root = ConfigNode.newInstance();
			for (String key : property.stringPropertyNames()) {
				root.set(key, property.getProperty(key));
			}
			configs.add(root);
		}
		return ConfigNode.union(configs);
	}

	private static List<Properties> loadProperties(List<Path> paths) {
		List<Properties> properties = new ArrayList<>();
		for (Path path : paths) {
			try (InputStream fis = Files.newInputStream(path)) {
				Properties property = new Properties();
				property.load(fis);
				properties.add(property);
			} catch (IOException e) {
				logger.warn("Can't load config: {}, {}", path, e.getMessage());
			}
		}
		return properties;
	}
}
