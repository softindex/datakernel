package io.datakernel.docs.model;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public final class MarkdownContent {
	private static final String PRESENT = "present";
	private static final String DELIMITER = "-";
	private final Map<String, String> properties;
	private final String content;

	private MarkdownContent(Map<String, String> properties, String content) {
		this.properties = properties;
		this.content = content;
	}

	public static MarkdownContent of(Map<String, String> properties, String content) {
		Map<String, String> copyProperties = new HashMap<>(properties);
		properties.forEach((key, value) -> copyProperties.put((key + DELIMITER + value), PRESENT));
		return new MarkdownContent(copyProperties, content);
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public String getContent() {
        return content;
    }
}
