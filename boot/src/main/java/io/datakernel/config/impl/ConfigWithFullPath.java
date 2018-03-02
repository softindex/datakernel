package io.datakernel.config.impl;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;

import java.util.NoSuchElementException;
import java.util.Set;

public class ConfigWithFullPath implements Config {
	private final String prefix;
	private final Config config;

	private ConfigWithFullPath(String prefix, Config config) {
		this.prefix = prefix;
		this.config = config;
	}

	public static ConfigWithFullPath wrap(Config config) {
		return new ConfigWithFullPath("", config);
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
			throw new NoSuchElementException(resolveFullPath(path));
		}
	}

	public String resolveFullPath(String path) {
		return (prefix.isEmpty() ? "" : prefix + DELIMITER) + path;
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
			throw new NoSuchElementException(path + DELIMITER + e.getMessage());
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
		return new ConfigWithFullPath(resolveFullPath(path), config.getChild(path));
	}

	@Override
	public Set<String> getChildren() {
		return config.getChildren();
	}

	@Override
	public boolean isEmpty() {
		return config.isEmpty();
	}
}
