package io.datakernel.config;

import io.datakernel.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Collections.*;

public interface Config {
	Logger logger = LoggerFactory.getLogger(Config.class);

	Config EMPTY = new Config() {
		@Override
		public String getValue(@Nullable String defaultValue) {
			return defaultValue;
		}

		@Override
		public Map<String, Config> getChildren() {
			return emptyMap();
		}
	};

	String THIS = "";
	String DELIMITER = ".";
	Pattern DELIMITER_PATTERN = Pattern.compile(Pattern.quote(DELIMITER));
	Pattern PATH_PATTERN = Pattern.compile("([0-9a-zA-Z_-]+(\\.[0-9a-zA-Z_-]+)*)?");

	static String concatPath(String prefix, String suffix) {
		return prefix.isEmpty() || suffix.isEmpty() ? prefix + suffix : prefix + DELIMITER + suffix;
	}

	static void checkPath(String path) {
		checkArgument(PATH_PATTERN.matcher(path).matches(), "Invalid path %s", path);
	}

	String getValue(@Nullable String defaultValue);

	Map<String, Config> getChildren();

	default boolean hasValue() {
		return getValue(null) != null;
	}

	default boolean hasChildren() {
		return !getChildren().isEmpty();
	}

	default boolean hasChild(String path) {
		checkPath(path);
		Config config = this;
		for (String key : DELIMITER_PATTERN.split(path)) {
			if (key.isEmpty()) continue;
			Map<String, Config> children = config.getChildren();
			if (!children.containsKey(key)) return false;
			config = children.get(key);
		}
		return true;
	}

	default boolean isEmpty() {
		return !hasValue() && !hasChildren();
	}

	default String get(String path) throws NoSuchElementException {
		checkPath(path);
		String value = get(path, null);
		if (value == null) {
			throw new NoSuchElementException();
		}
		return value;
	}

	default String get(String path, @Nullable String defaultValue) {
		return getChild(path).getValue(defaultValue);
	}

	default <T> T get(ConfigConverter<T> converter, String path) throws NoSuchElementException {
		return converter.get(getChild(path));
	}

	default <T> T get(ConfigConverter<T> converter, String path, @Nullable T defaultValue) {
		return converter.get(getChild(path), defaultValue);
	}

	default Config getChild(String path) {
		checkPath(path);
		Config config = this;
		for (String key : path.split(Pattern.quote(DELIMITER))) {
			if (key.isEmpty()) continue;
			Map<String, Config> children = config.getChildren();
			config = children.containsKey(key) ? children.get(key) : config.provideNoKeyChild(key);
		}
		return config;
	}

	default Config provideNoKeyChild(String key) {
		checkArgument(!getChildren().containsKey(key));
		return EMPTY;
	}

	default <T> void apply(ConfigConverter<T> converter, String path, Consumer<T> setter) {
		checkPath(path);
		T value = get(converter, path);
		setter.accept(value);
	}

	default <T> void apply(ConfigConverter<T> converter, String path, T defaultValue, Consumer<T> setter) {
		apply(converter, path, defaultValue, (value, $) -> setter.accept(value));
	}

	default <T> void apply(ConfigConverter<T> converter, String path, T defaultValue, BiConsumer<T, T> setter) {
		checkPath(path);
		T value = get(converter, path, defaultValue);
		setter.accept(value, defaultValue);
	}

	static <T> BiConsumer<T, T> ifNotDefault(Consumer<T> setter) {
		return (value, defaultValue) -> {
			if (!Objects.equals(value, defaultValue)) {
				setter.accept(value);
			}
		};
	}

	static <T> Consumer<T> ifNotDefault(T defaultValue, Consumer<T> setter) {
		return value -> {
			if (!Objects.equals(value, defaultValue)) {
				setter.accept(value);
			}
		};
	}

	static Config create() {
		return EMPTY;
	}

	static Config ofProperties(Properties properties) {
		return ofMap(properties.stringPropertyNames().stream()
				.collect(Collectors.toMap(k -> k, properties::getProperty,
						(u, v) -> {throw new AssertionError();}, LinkedHashMap::new)));
	}

	static Config ofProperties(String fileName) {
		return ofProperties(fileName, false);
	}

	static Config ofProperties(String fileName, boolean optional) {
		return ofProperties(Paths.get(fileName), optional);
	}

	static Config ofProperties(Path file, boolean optional) {
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

	static Config ofMap(Map<String, String> map) {
		Config config = create();
		for (String path : map.keySet()) {
			String value = map.get(path);
			config = config.with(path, value);
		}
		return config;
	}

	static Config ofConfigs(Map<String, Config> map) {
		Config config = create();
		for (String path : map.keySet()) {
			Config childConfig = map.get(path);
			config = config.with(path, childConfig);
		}
		return config;
	}

	static Config ofValue(String value) {
		return create().with(THIS, value);
	}

	static <T> Config ofValue(ConfigConverter<T> configConverter, T value) {
		EffectiveConfig effectiveConfig = EffectiveConfig.wrap(Config.create());
		configConverter.get(effectiveConfig, value);
		return ofMap(effectiveConfig.getEffectiveDefaults());
	}

	default Config with(String path, String value) {
		checkPath(path);
		checkNotNull(value);
		return with(path, new Config() {
			@Override
			public String getValue(@Nullable String defaultValue) {
				return value;
			}

			@Override
			public Map<String, Config> getChildren() {
				return emptyMap();
			}
		});
	}

	default Config with(String path, Config config) {
		checkPath(path);
		checkNotNull(config);
		String value = config.getValue(null);
		String[] keys = path.split(Pattern.quote(DELIMITER));
		for (int i = keys.length - 1; i >= 0; i--) {
			String key = keys[i];
			if (key.isEmpty()) continue;
			Map<String, Config> map = singletonMap(key, config);
			config = new Config() {
				@Override
				public String getValue(@Nullable String defaultValue) {
					return defaultValue;
				}

				@Override
				public Map<String, Config> getChildren() {
					return map;
				}
			};
		}
		return override(config);
	}

	default Config override(Config other) {
		String otherValue = other.getValue(null);
		Map<String, Config> otherChildren = other.getChildren();
		if (otherValue == null && otherChildren.isEmpty()) {
			return this;
		}
		String value = otherValue != null ? otherValue : getValue(null);
		Map<String, Config> children = new LinkedHashMap<>(getChildren());
		otherChildren.keySet().forEach(key -> children.merge(key, otherChildren.get(key), Config::override));
		Map<String, Config> finalChildren = unmodifiableMap(children);
		return new Config() {
			@Override
			public String getValue(@Nullable String defaultValue) {
				return value != null ? value : defaultValue;
			}

			@Override
			public Map<String, Config> getChildren() {
				return finalChildren;
			}
		};
	}

	default Map<String, String> toMap() {
		Map<String, String> result = new LinkedHashMap<>();
		if (hasValue()) {
			result.put(THIS, get(THIS));
		}
		Map<String, Config> children = getChildren();
		for (String key : children.keySet()) {
			Map<String, String> childMap = children.get(key).toMap();
			result.putAll(childMap.entrySet().stream()
					.collect(Collectors.toMap(entry -> concatPath(key, entry.getKey()), Map.Entry::getValue)));
		}
		return result;
	}

	default Properties toProperties() {
		Properties properties = new Properties();
		toMap().forEach(properties::setProperty);
		return properties;
	}

}
