package io.datakernel.docs.plugin.text;

import java.util.List;

public interface TextPlugin {
	String apply(String innerContent, List<String> params) throws PluginApplyException;

	String getName();
}
