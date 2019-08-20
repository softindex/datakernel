package io.datakernel.docs.plugin.props;

import io.datakernel.docs.model.MarkdownContent;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Map;

public interface PropertiesPlugin<T> {
	Map<String, T> apply(@Nullable MarkdownContent markdownContent) throws IOException;

	String getName();
}
