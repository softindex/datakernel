package io.global.blog.preprocessor;

import io.global.blog.http.view.PostView;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CommentsPreprocessor implements Preprocessor<PostView> {
	private static final Pattern REPLY_LINK_PATTERN = Pattern.compile("\\[(?<username>.*?)]\\s*\\(#(?<postId>.*?)\\)");
	private static final String LINK_PATTERN = "<a href=\"#post_%s\">%s</a>";

	private static String escapeHTML(String text) {
		StringBuilder out = new StringBuilder();
		for (char symb : text.toCharArray()) {
			if (symb > 127 || symb == '"' || symb == '<' || symb == '>' || symb == '&') {
				out.append("&#");
				out.append((int) symb);
				out.append(';');
			} else {
				out.append(symb);
			}
		}
		return out.toString();
	}

	@Override
	public PostView process(PostView instance, Object... contextParams) {
		for (PostView child : instance.getChildren()) {
			process(child, contextParams);
		}
		String content = instance.getContent();
		StringBuilder contentBuilder = new StringBuilder();
		Matcher matcher = REPLY_LINK_PATTERN.matcher(content);
		int offset = 0;
		if (matcher.find()) {
			contentBuilder.append(escapeHTML(content.substring(offset, matcher.start())));
			String username = matcher.group("username");
			String postId = matcher.group("postId");
			contentBuilder.append(String.format(LINK_PATTERN, postId, username))
					.append(",");
			offset = matcher.end();
		}
		contentBuilder.append(escapeHTML(content.substring(offset)));
		return instance.withRenderedContent(contentBuilder.toString());
	}
}
