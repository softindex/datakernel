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

import io.datakernel.annotation.Nullable;

import java.util.*;
import java.util.regex.Pattern;

import static io.datakernel.config.Config.ConflictResolver.PROHIBIT_COLLISIONS;
import static io.datakernel.config.Config.ConflictResolver.RETURN_LAST_FOUND;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public interface Config {
	String THIS = "";
	String DELIMITER = ".";

	boolean hasValue();

	String get(String path);

	String get(String path, @Nullable String defaultValue);

	<T> T get(ConfigConverter<T> converter, String path);

	<T> T get(ConfigConverter<T> converter, String path, @Nullable T defaultValue);

	boolean hasChild(String path);

	Config getChild(String path);

	Set<String> getChildren();

	default Config with(String key, Config config) {
		String[] keys = key.split(Pattern.quote(DELIMITER));
		for (int i = keys.length - 1; i >= 0; i--) {
			config = ofChild(keys[i], config);
		}
		return override(config);
	}

	default Config with(String key, String value) {
		return with(key, ofValue(value));
	}

	default Config override(Config other) {
		return union(RETURN_LAST_FOUND, this, other);
	}

	default Config combine(Config other) {
		return union(PROHIBIT_COLLISIONS, this, other);
	}

	default String getValue() {
		return get(THIS);
	}

	default boolean isEmpty() {
		return this == EMPTY;
	}

	@Nullable
	default <T> T getOrNull(ConfigConverter<T> converter, String path) {
		Config child = getChild(path);
		if (child.isEmpty()) {
			return null;
		}
		return get(converter, path);
	}

	Config EMPTY = new AbstractConfig() {
		@Override
		protected String doGet() {
			throw new NoSuchElementException();
		}

		@Override
		protected String doGet(String defaultString) {
			return defaultString;
		}

		@Override
		public boolean hasValue() {
			return false;
		}

		@Override
		protected boolean doHasChild(String key) {
			return false;
		}

		@Override
		protected Config doGetChild(String key) {
			return EMPTY;
		}

		@Override
		public Set<String> getChildren() {
			return emptySet();
		}

		@Override
		public String toString() {
			return "empty";
		}
	};

	static Config create() {
		return EMPTY;
	}

	static Config ofValue(String value) {
		return new AbstractConfig() {
			@Override
			protected String doGet() {
				return value;
			}

			@Override
			protected String doGet(String defaultString) {
				return value;
			}

			@Override
			public boolean hasValue() {
				return true;
			}

			@Override
			protected boolean doHasChild(String key) {
				return false;
			}

			@Override
			protected Config doGetChild(String key) {
				return EMPTY;
			}

			@Override
			public Set<String> getChildren() {
				return emptySet();
			}

			@Override
			public String toString() {
				return value;
			}
		};
	}

	static <T> Config ofValue(ConfigConverter<T> configConverter, T value) {
		EffectiveConfig effectiveConfig = EffectiveConfig.wrap(Config.create());
		configConverter.get(effectiveConfig, value);
		Map<String, String> effectiveDefaults = effectiveConfig.getEffectiveDefaults();
		return ofMap(effectiveDefaults);
	}

	@FunctionalInterface
	interface ConflictResolver {

		ConflictResolver RETURN_FIRST_FOUND = configs -> configs.get(0);

		ConflictResolver RETURN_LAST_FOUND = configs -> configs.get(configs.size() - 1);

		ConflictResolver PROHIBIT_COLLISIONS = configs -> {
			throw new IllegalStateException("More than one config value for path " + configs);
		};

		Config resolve(List<? extends Config> configs);
	}

	static Config union(ConflictResolver resolver, Config... configs) {
		return union(resolver, asList(configs));
	}

	static Config union(ConflictResolver resolver, List<Config> configList) {
		List<Config> configs = configList.stream().filter(c -> !c.isEmpty()).collect(toList());
		if (configs.isEmpty()) {
			return EMPTY;
		}
		if (configs.size() == 1) {
			return configs.get(0);
		}
		return new AbstractConfig() {

			@Override
			protected String doGet() {
				return findConfigValue(resolver, configs).get(THIS);
			}

			@Override
			protected String doGet(String defaultString) {
				return findConfigValue(resolver, configs).get(THIS, defaultString);
			}

			@Override
			public boolean hasValue() {
				return configs.stream().anyMatch(Config::hasValue);
			}

			@Override
			protected boolean doHasChild(String key) {
				return configs.stream().anyMatch(input -> input.hasChild(key));
			}

			@Override
			public Config doGetChild(String key) {
				List<Config> childConfigs = new ArrayList<>();
				for (Config config : configs) {
					childConfigs.add(config.getChild(key));
				}
				if (childConfigs.stream().allMatch(c -> c.hasValue() && c.getChildren().isEmpty())) {
					return resolver.resolve(childConfigs);
				}
				return Config.union(resolver, childConfigs);
			}

			@Override
			public Set<String> getChildren() {
				Set<String> children = new HashSet<>();
				for (Config config : configs) {
					children.addAll(config.getChildren());
				}
				return children;
			}

			@Override
			public String toString() {
				return "Union" + configs;
			}

			private Config findConfigValue(ConflictResolver resolver, List<? extends Config> configs) {
				List<Config> appropriateConfigs = configs.stream()
						.filter(Config::hasValue)
						.collect(toList());
				return appropriateConfigs.isEmpty() ?
						EMPTY :
						appropriateConfigs.size() == 1 ?
								appropriateConfigs.get(0) :
								resolver.resolve(appropriateConfigs);
			}
		};
	}

	static Config ofChild(String path, Config child) {
		return ofChildren(singletonMap(path, child));
	}

	static Config ofChildren(Map<String, Config> map) {
		return new AbstractConfig() {
			@Override
			protected String doGet() throws NoSuchElementException {
				throw new NoSuchElementException();
			}

			@Override
			protected String doGet(String defaultString) {
				return defaultString;
			}

			@Override
			public boolean hasValue() {
				return false;
			}

			@Override
			protected boolean doHasChild(String key) {
				return map.containsKey(key);
			}

			@Override
			public Config doGetChild(String key) {
				return map.getOrDefault(key, EMPTY);
			}

			@Override
			public Set<String> getChildren() {
				return map.keySet();
			}

			@Override
			public boolean isEmpty() {
				return map.isEmpty();
			}

			@Override
			public String toString() {
				return "MapConfig" + map;
			}
		};
	}

	static Config ofMap(Map<String, String> map) {
		TreeConfig treeConfig = new TreeConfig();
		for (String key : map.keySet()) {
			String value = map.get(key);
			if (value != null) {
				treeConfig.add(key, value);
			} else {
				treeConfig.addBranch(key);
			}
		}
		return treeConfig;
	}
}
