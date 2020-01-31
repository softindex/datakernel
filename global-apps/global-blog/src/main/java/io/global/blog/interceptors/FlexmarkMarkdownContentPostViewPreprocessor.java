package io.global.blog.interceptors;

import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Document;
import io.global.blog.http.view.PostView;

public class FlexmarkMarkdownContentPostViewPreprocessor implements Preprocessor<PostView> {
	private final HtmlRenderer renderer;
	private final Parser parser;

	public FlexmarkMarkdownContentPostViewPreprocessor(HtmlRenderer renderer, Parser parser) {
		this.renderer = renderer;
		this.parser = parser;
	}

	@Override
	public PostView process(PostView postView, Object... params) {
		String markdown = postView.getContent();
		Document doc = parser.parse(markdown);
		postView.withRenderedContent(renderer.render(doc));
		return postView;
	}
}
