package io.datakernel.docs.plugin.text;

import java.util.List;
import java.util.Map;

import static io.datakernel.docs.plugin.TextEngineRenderer.PROPERTIES;

public final class PropertiesTextPlugin implements TextPlugin {
	private final Map<String, String> properties;

	public PropertiesTextPlugin(Map<String, String> properties) {
		this.properties = properties;
	}

	@Override
	public String apply(String innerContent, List<String> params) throws PluginApplyException {
		if (params.size() < 1) {
			throw new PluginApplyException("Not enough params");
		}
		String key = params.get(0);
		String value = properties.get(key);
		if (value == null) {
			throw new PluginApplyException("Cannot find value for the key " + key);
		}
		return value;
	}

	@Override
	public String getName() {
		return PROPERTIES;
	}
}
