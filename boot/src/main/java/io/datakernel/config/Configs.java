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

import java.util.*;

public final class Configs {
	public interface ConflictResolver {
		// is called iff 2+ found
		Config resolve(List<? extends Config> configs);
	}

	private Configs() {
	}

	public static final ConflictResolver RETURN_FIRST_FOUND = configs -> configs.get(0);

	public static final ConflictResolver RETURN_LAST_FOUND = configs -> configs.get(configs.size() - 1);

	public static final ConflictResolver PROHIBIT_COLLISIONS = configs -> {
		throw new IllegalStateException("many config values for the same path: " + configs);
	};

	public static final Config EMPTY_CONFIG = new AbstractConfig() {
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
			return EMPTY_CONFIG;
		}

		@Override
		public Set<String> getChildren() {
			return Collections.emptySet();
		}

		@Override
		public String toString() {
			return "empty";
		}
	};

	public static Config emptyConfig() {
		return EMPTY_CONFIG;
	}

	public static Config ofValue(String value) {
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
				return EMPTY_CONFIG;
			}

			@Override
			public Set<String> getChildren() {
				return Collections.emptySet();
			}

			@Override
			public String toString() {
				return value;
			}
		};
	}

	public static Config union(ConflictResolver resolver, Config... configs) {
		return union(resolver, Arrays.asList(configs));
	}

	public static Config union(ConflictResolver resolver, List<? extends Config> allConfigs) {
		return new AbstractConfig() {
			@Override
			protected String doGet() {
				return findConfig(resolver, allConfigs).get(THIS);
			}

			@Override
			protected String doGet(String defaultString) {
				return findConfig(resolver, allConfigs).get(THIS, defaultString);
			}

			@Override
			public boolean hasValue() {
				return allConfigs.stream().anyMatch(Config::hasValue);
			}

			@Override
			protected boolean doHasChild(String key) {
				return allConfigs.stream().anyMatch(input -> input.hasChild(key));
			}

			@Override
			public Config doGetChild(String key) {
				List<Config> childConfigs = new ArrayList<>();
				for (Config config : allConfigs) {
					childConfigs.add(config.getChild(key));
				}
				return union(resolver, childConfigs);
			}

			@Override
			public Set<String> getChildren() {
				Set<String> children = new HashSet<>();
				for (Config config : allConfigs) {
					children.addAll(config.getChildren());
				}
				return children;
			}

			@Override
			public String toString() {
				return "Union[" + allConfigs.toString() + "]";
			}

			private Config findConfig(ConflictResolver resolver, List<? extends Config> configs) {
				List<Config> appropriateConfigs = new ArrayList<>();
				for (Config config : configs) {
					if (config.hasValue()) {
						appropriateConfigs.add(config);
					}
				}
				if (appropriateConfigs.isEmpty()) {
					return EMPTY_CONFIG;
				} else if (appropriateConfigs.size() == 1) {
					return appropriateConfigs.get(0);
				}
				return resolver.resolve(appropriateConfigs);
			}
		};
	}

	public static Config ofMap(Map<String, ? extends Config> map) {
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
				return map.containsKey(key) ? map.get(key) : emptyConfig();
			}

			@Override
			public Set<String> getChildren() {
				return map.keySet();
			}

			@Override
			public String toString() {
				return "MapConfig[" + map.toString() + "]";
			}
		};
	}
}
