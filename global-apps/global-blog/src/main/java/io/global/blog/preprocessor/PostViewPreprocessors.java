package io.global.blog.preprocessor;

import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;

public final class PostViewPreprocessors {
	public static FlexmarkMarkdownContentPostViewPreprocessor renderedContent(HtmlRenderer renderer, Parser parser) {
		return new FlexmarkMarkdownContentPostViewPreprocessor(renderer, parser);
	}

	public static FlexmarkMarkdownPreviewPostViewPreprocessor renderedPreview(HtmlRenderer renderer, Parser parser) {
		return new FlexmarkMarkdownPreviewPostViewPreprocessor(renderer, parser);
	}

	public static LinkAttachmentPostViewPreprocessor replaceAttachmentLinks() {
		return new LinkAttachmentPostViewPreprocessor();
	}

	public static CommentsPreprocessor commentsPreprocessor() {
		return new CommentsPreprocessor();
	}
}
