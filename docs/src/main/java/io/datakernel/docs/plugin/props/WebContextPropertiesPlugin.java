package io.datakernel.docs.plugin.props;

import io.datakernel.docs.model.MarkdownContent;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class WebContextPropertiesPlugin implements PropertiesPlugin<String> {
	private final Map<String, String> properties;

	private WebContextPropertiesPlugin(Map<String, String> properties) {
		this.properties = properties;
	}

	public static WebContextPropertiesPlugin create(Map<String, String> properties) {
		return new WebContextPropertiesPlugin(properties);
	}

	@Override
	public Map<String, String> apply(@Nullable MarkdownContent markdownContent) {
		return properties;
	}

	@Override
	public String getName() {
		return "context";
	}
}
