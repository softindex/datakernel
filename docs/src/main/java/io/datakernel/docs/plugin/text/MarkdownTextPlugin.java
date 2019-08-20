package io.datakernel.docs.plugin.text;

import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Document;

import java.util.List;

public class MarkdownTextPlugin implements TextPlugin {
	private static final String TAG = "markdown";
	private final HtmlRenderer htmlRenderer;
	private final Parser parser;

	public MarkdownTextPlugin(HtmlRenderer htmlRenderer, Parser parser) {
		this.htmlRenderer = htmlRenderer;
		this.parser = parser;
	}

	@Override
	public String apply(String innerContent, List<String> params) {
		Document doc = parser.parse(innerContent);
		return htmlRenderer.render(doc);
	}

	@Override
	public String getName() {
		return TAG;
	}
}
