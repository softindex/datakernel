package io.datakernel.docs.plugin.text;

import java.util.List;
import java.util.UUID;

import static java.lang.String.format;

public final class GraphVizTextPlugin implements TextPlugin {
	private static final String TAG = "graph-viz";
	private static final String tagFormat =
			"<div id=\"%1$s\">%2$s</div>\n" +
			"<script src=\"%3$s\"></script>\n" +
			"<script src=\"%4$s\"></script>\n" +
			"<script>\n" +
			"    new Viz().renderSVGElement(document.getElementById(\"%1$s\").innerText)\n" +
			"      .then(function(element) {\n" +
			"        document.getElementById(\"%1$s\").innerText = null;\n" +
			"        document.getElementById(\"%1$s\").appendChild(element);\n" +
			"      });\n" +
			"</script>";
	private final String vizJsPath;
	private final String fullVizJsPath;

	public GraphVizTextPlugin(String vizJsPath, String fullVizJsPath) {
		this.vizJsPath = vizJsPath;
		this.fullVizJsPath = fullVizJsPath;
	}

	@Override
	public String apply(String innerContent, List<String> params) {
		return format(tagFormat, UUID.randomUUID().toString(), innerContent, vizJsPath, fullVizJsPath);
	}

	@Override
	public String getName() {
		return TAG;
	}
}
