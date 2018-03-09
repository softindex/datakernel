package io.datakernel.config.impl;

import io.datakernel.config.Config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.datakernel.config.Config.concatPath;
import static io.datakernel.util.Preconditions.checkArgument;

public class ConfigWithFullPath implements Config {
	private final String prefix;
	private final Config config;
	private final Map<String, Config> children;

	private ConfigWithFullPath(String prefix, Config config) {
		this.prefix = prefix;
		this.config = config;
		this.children = new LinkedHashMap<>();
		for (Map.Entry<String, Config> entry : config.getChildren().entrySet()) {
			this.children.put(entry.getKey(),
					new ConfigWithFullPath(concatPath(this.prefix, entry.getKey()), entry.getValue()));
		}
	}

	public static ConfigWithFullPath wrap(Config config) {
		return new ConfigWithFullPath("", config);
	}

	@Override
	public String getValue(String defaultValue) {
		return config.getValue(defaultValue);
	}

	@Override
	public Map<String, Config> getChildren() {
		return children;
	}

	@Override
	public Config provideNoKeyChild(String key) {
		checkArgument(!children.keySet().contains(key));
		return new ConfigWithFullPath(concatPath(this.prefix, key), EMPTY);
	}

	@Override
	public String get(String path) {
		try {
			return config.get(path);
		} catch (NoSuchElementException e) {
			throw new NoSuchElementException(concatPath(this.prefix, path));
		}
	}
}
