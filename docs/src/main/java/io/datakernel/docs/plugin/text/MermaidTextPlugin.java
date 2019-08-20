package io.datakernel.docs.plugin.text;

import java.util.List;

import static java.lang.String.format;

public final class MermaidTextPlugin implements TextPlugin {
	private static final String TAG = "mermaid";
	private static final String script =
			"<script src=\"%s\"></script>" +
			"<div class=\"mermaid\">%s</div>";
	private final String mermaidJsPath;

	public MermaidTextPlugin(String mermaidJsPath) {
		this.mermaidJsPath = mermaidJsPath;
	}

	@Override
	public String apply(String innerContent, List<String> params) {
		return format(script, mermaidJsPath, innerContent);
	}

	@Override
	public String getName() {
		return TAG;
	}
}
