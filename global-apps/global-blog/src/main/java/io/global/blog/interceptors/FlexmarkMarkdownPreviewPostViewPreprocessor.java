package io.global.blog.interceptors;

import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Document;
import io.global.blog.http.view.PostView;

import static java.lang.Math.min;

public final class FlexmarkMarkdownPreviewPostViewPreprocessor implements Preprocessor<PostView> {
	private static final int DEFAULT_PREVIEW_LENGTH = 356;
	private static final String START_PREVIEW = "\n\n";

	private final HtmlRenderer renderer;
	private final Parser parser;

	public FlexmarkMarkdownPreviewPostViewPreprocessor(HtmlRenderer renderer, Parser parser) {
		this.renderer = renderer;
		this.parser = parser;
	}

	@Override
	public PostView process(PostView postView, Object... params) {
		String content = postView.getContent();

		String previewContent;
		int start = content.indexOf(START_PREVIEW);
		if (start == -1) {
			previewContent = content.substring(0, min(DEFAULT_PREVIEW_LENGTH, content.length()));
		} else {
			int end = content.indexOf(START_PREVIEW, start + START_PREVIEW.length());
			int defaultOffset = start + DEFAULT_PREVIEW_LENGTH;
			end = (end != -1 ? end : min(defaultOffset, content.length()));
			previewContent = content.substring(start, end);
		}
		Document doc = parser.parse(previewContent + "...");
		postView.withRenderedContent(renderer.render(doc));
		return postView;
	}
}
