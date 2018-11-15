package io.datakernel.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.datakernel.config.Config.concatPath;
import static io.datakernel.util.Preconditions.checkArgument;

public class ConfigWithFullPath implements Config {
	private final String path;
	private final Config config;
	private final Map<String, Config> children;

	private ConfigWithFullPath(String path, Config config) {
		this.path = path;
		this.config = config;
		this.children = new LinkedHashMap<>();
		config.getChildren().forEach((key, value) ->
				this.children.put(key, new ConfigWithFullPath(concatPath(this.path, key), value)));
	}

	public static ConfigWithFullPath wrap(Config config) {
		return new ConfigWithFullPath("", config);
	}

	@Override
	public String getValue(String defaultValue) {
		return config.getValue(defaultValue);
	}

	@Override
	public String getValue() throws NoSuchElementException {
		try {
			return config.getValue();
		} catch (NoSuchElementException ignored) {
			throw new NoSuchElementException(path);
		}
	}

	@Override
	public Map<String, Config> getChildren() {
		return children;
	}

	@Override
	public Config provideNoKeyChild(String key) {
		checkArgument(!children.keySet().contains(key), "Children already contain key '%s'", key);
		return new ConfigWithFullPath(concatPath(path, key), config.provideNoKeyChild(key));
	}

}
