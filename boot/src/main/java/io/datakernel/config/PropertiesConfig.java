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
	public static class Builder {
		private List<Path> required = new ArrayList<>();
		private List<Path> optional = new ArrayList<>();
		private List<Properties> properties = new ArrayList<>();
		private Map<TypeToken, ConfigConverter> converters = new HashMap<>();
		private Path saveConfigPath;

		private Builder() {

		}

		public Builder addFile(String path, boolean required) {
			this.required.add(Paths.get(path));
			return this;
		}

		public Builder addFile(Path path, boolean required) {
			this.required.add(path);
			return this;
		}

		public Builder addProperties(Properties property) {
			properties.add(property);
			return this;
		}

		public Builder saveConfig(Path saveConfigPath) {
			this.saveConfigPath = saveConfigPath;
			return this;
		}

		public Builder saveConfig(String saveConfigPath) {
			this.saveConfigPath = Paths.get(saveConfigPath);
			return this;
		}

		public <T> Builder registerConfigConverter(TypeToken<T> type, ConfigConverter<T> converter) {
			converters.put(type, converter);
			return this;
		}

		public <T> Builder registerConfigConverter(Class<T> type, ConfigConverter<T> converter) {
			converters.put(TypeToken.of(type), converter);
			return this;
		}

		public PropertiesConfig build() throws IOException {
			loadProperties(required, true);
			loadProperties(optional, false);

			ConfigTree root = buildTree();

			for (Map.Entry<TypeToken, ConfigConverter> entry : converters.entrySet()) {
				root.registerConverter(entry.getKey(), entry.getValue());
			}

			return new PropertiesConfig(root, saveConfigPath);
		}

		private void loadProperties(List<Path> src, boolean required) throws IOException {
			for (Path path : src) {
				try (InputStream fis = Files.newInputStream(path)) {
					Properties property = new Properties();
					property.load(fis);
					properties.add(property);
				} catch (IOException e) {
					if (required) {
						throw new IOException("Can't load required properties file", e);
					} else {
						logger.warn("Can't load optional properties file: {}", path);
					}
				}
			}
		}

		private ConfigTree buildTree() {
			List<ConfigTree> configs = new ArrayList<>();
			for (Properties property : properties) {
				ConfigTree root = ConfigTree.newInstance();
				for (String key : property.stringPropertyNames()) {
					root.set(key, property.getProperty(key));
				}
				configs.add(root);
			}
			return ConfigTree.union(configs);
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(PropertiesConfig.class);

	private final ConfigTree root;
	private final Path saveConfigPath;

	// creators
	private PropertiesConfig(ConfigTree root, Path saveConfigPath) {
		this.root = root;
		this.saveConfigPath = saveConfigPath;
	}

	public static Builder build() {
		return new Builder();
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
}
