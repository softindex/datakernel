package io.global.blog.interceptors;

import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Document;
import io.global.blog.http.view.PostView;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FlexmarkMarkdownPreviewPostViewPreprocessor implements Preprocessor<PostView> {
	private static final int DEFAULT_PREVIEW_LENGTH = 356;
	private static final int MAX_PREVIEW_LENGTH = 512;
	private static final Pattern MEDIA_LINK_PATTERN = Pattern.compile("!([PV])\\[.+?]\\(.*?\\)");

	private final HtmlRenderer renderer;
	private final Parser parser;

	public FlexmarkMarkdownPreviewPostViewPreprocessor(HtmlRenderer renderer, Parser parser) {
		this.renderer = renderer;
		this.parser = parser;
	}

	@Override
	public PostView process(PostView postView, Object... params) {
		String content = postView.getContent();
		Matcher matcher = MEDIA_LINK_PATTERN.matcher(content);
		String previewContent;
		if (matcher.find() && matcher.end() < MAX_PREVIEW_LENGTH) {
			previewContent = content.substring(0, matcher.end());
		} else if (content.length() < DEFAULT_PREVIEW_LENGTH){
			previewContent = content;
		} else {
			previewContent = content.substring(0, DEFAULT_PREVIEW_LENGTH) + "...";
		}

		// TODO replace with some proper markdown escape
		previewContent = escapeHeaders(previewContent);

		Document doc = parser.parse(previewContent);
		postView.withRenderedContent(renderer.render(doc));
		return postView;
	}

	private static final Pattern HEADINGS_PATTERN = Pattern.compile("#+");
	private static String escapeHeaders(String content){
		return HEADINGS_PATTERN.matcher(content).replaceAll("");
	}
}
