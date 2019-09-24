package io.global.blog.preprocessor;

import io.global.blog.http.view.PostView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.global.blog.util.Utils.castIfExist;

public final class LinkAttachmentPostViewPreprocessor implements Preprocessor<PostView> {
	private static final Pattern LINK_PATTERN = Pattern.compile("\\(attach:(.*?)(\\s+\".*?\")?\\)");
	private static final Logger logger = LoggerFactory.getLogger(LinkAttachmentPostViewPreprocessor.class);

	@Override
	public PostView process(PostView postView, Object... params) {
		String threadId = castIfExist(params[0], String.class);
		if (threadId == null) {
			logger.warn("Cannot find threadId for attachments");
			return postView;
		}
		String content = postView.getContent();
		Matcher matcher = LINK_PATTERN.matcher(content);
		if (!matcher.find()) {
			return postView;
		}
		StringBuilder contentBuilder = new StringBuilder(content);
		int offset = 0;
		do  {
			String attachmentFileName = matcher.group(1);
			String restLink = matcher.group(2);
			String newLink = "(" + threadId + "/" + postView.getPostId() + "/download/" + attachmentFileName + (restLink != null ? restLink : "") + ")";
			contentBuilder.replace(matcher.start() + offset, matcher.end() + offset, newLink);
			offset = newLink.length() - matcher.group().length();
		} while (matcher.find());
		postView.withContent(contentBuilder.toString());
		return postView;
	}
}
