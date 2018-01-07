package io.datakernel.config.impl;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;

import java.util.NoSuchElementException;
import java.util.Set;

public class ConfigWithPrefix implements Config {
	private final String prefix;
	private final Config config;

	private ConfigWithPrefix(String prefix, Config config) {
		this.prefix = prefix;
		this.config = config;
	}

	public static ConfigWithPrefix create(String prefix, Config config) {
		return new ConfigWithPrefix(prefix, config);
	}

	public static ConfigWithPrefix createRoot(Config config) {
		return new ConfigWithPrefix("", config);
	}

	@Override
	public boolean hasValue() {
		return config.hasValue();
	}

	@Override
	public String get(String path) {
		try {
			return config.get(path);
		} catch (NoSuchElementException e) {
			throw new NoSuchElementException(getFullPath(path));
		}
	}

	private String getFullPath(String path) {
		return (prefix.isEmpty() ? "" : prefix + ".") + path;
	}

	@Override
	public String get(String path, String defaultValue) {
		return config.get(path, defaultValue);
	}

	@Override
	public <T> T get(ConfigConverter<T> converter, String path) {
		try {
			return config.get(converter, path);
		} catch (NoSuchElementException e) {
			throw new NoSuchElementException(path + "." + e.getMessage());
		}
	}

	@Override
	public <T> T get(ConfigConverter<T> converter, String path, T defaultValue) {
		return config.get(converter, path, defaultValue);
	}

	@Override
	public boolean hasChild(String path) {
		return config.hasChild(path);
	}

	@Override
	public Config getChild(String path) {
		return create(getFullPath(path), config.getChild(path));
	}

	@Override
	public Set<String> getChildren() {
		return config.getChildren();
	}
}
