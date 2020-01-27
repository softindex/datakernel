package io.global.blog;

import com.vladsch.flexmark.ext.anchorlink.AnchorLinkExtension;
import com.vladsch.flexmark.ext.autolink.AutolinkExtension;
import com.vladsch.flexmark.ext.jekyll.front.matter.JekyllFrontMatterExtension;
import com.vladsch.flexmark.ext.media.tags.MediaTagsExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.data.MutableDataSet;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpException;
import io.datakernel.http.di.RouterModule.Mapped;
import io.datakernel.promise.Promise;
import io.global.comm.dao.CommDao;
import io.global.comm.dao.ThreadDao;
import io.global.comm.http.CommServletModule;
import io.global.comm.http.CommServletModule.PostRenderer;
import io.global.comm.http.CommServletModule.PrivilegePredicate;
import io.global.comm.http.view.ThreadView;
import io.global.comm.pojo.Post;
import io.global.comm.pojo.UserRole;
import io.global.mustache.MustacheTemplater;
import io.global.ot.session.UserId;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.vladsch.flexmark.ext.anchorlink.AnchorLinkExtension.ANCHORLINKS_WRAP_TEXT;
import static com.vladsch.flexmark.html.HtmlRenderer.RENDER_HEADER_ID;
import static com.vladsch.flexmark.parser.Parser.EXTENSIONS;
import static com.vladsch.flexmark.parser.Parser.PARSER_EMULATION_PROFILE;
import static com.vladsch.flexmark.parser.ParserEmulationProfile.GITHUB_DOC;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.di.RouterModule.MappedHttpMethod.POST;
import static io.global.comm.http.CommServletModule.attachThreadDao;
import static io.global.comm.http.CommServletModule.handleAttachments;
import static java.util.Arrays.asList;

public final class BlogServletModule extends AbstractModule {
	private static final int DEFAULT_PREVIEW_LENGTH = 356;
	private static final int MAX_PREVIEW_LENGTH = 512;
	private static final Pattern MEDIA_TAG_PATTERN = Pattern.compile("!(?:[PV])?\\[.+?]\\(.*?\\)");
	private static final Pattern ATTACHMENT_LINK_PATTERN = Pattern.compile("\\(attachment://(.*?)\\)");
	private static final Pattern HEADING_PATTERN = Pattern.compile("^\\s*#+");

	private BlogServletModule() {
	}

	public static Module create() {
		return CommServletModule.create("BLOG_SID", 5, 10, 10, 2)
				.overrideWith(new BlogServletModule());
	}

	@Provides
	HtmlRenderer htmlRenderer(MutableDataSet options) {
		return HtmlRenderer.builder(options).build();
	}

	@Provides
	Parser markdownParser(MutableDataSet options) {
		return Parser.builder(options).build();
	}

	@Provides
	MutableDataSet options() {
		MutableDataSet options = new MutableDataSet();
		options.set(PARSER_EMULATION_PROFILE, GITHUB_DOC);
		options.set(RENDER_HEADER_ID, true);
		options.set(ANCHORLINKS_WRAP_TEXT, false);
		options.set(EXTENSIONS, asList(
				MediaTagsExtension.create(),
				AnchorLinkExtension.create(),
				JekyllFrontMatterExtension.create(),
				AutolinkExtension.create()));
		return options;
	}

	private static String renderPreview(String content, HtmlRenderer renderer, Parser parser) {
		Matcher matcher = MEDIA_TAG_PATTERN.matcher(content);
		String previewContent;
		if (matcher.find() && matcher.end() < MAX_PREVIEW_LENGTH) {
			previewContent = content.substring(0, matcher.end());
		} else if (content.length() < DEFAULT_PREVIEW_LENGTH) {
			previewContent = content;
		} else {
			previewContent = content.substring(0, DEFAULT_PREVIEW_LENGTH) + "...";
		}
		return renderer.render(parser.parse(HEADING_PATTERN.matcher(previewContent).replaceAll("")));
	}

	private static String replaceAttachmentMedia(String threadId, Post post) {
		return ATTACHMENT_LINK_PATTERN.matcher(post.getContent()).replaceAll("(" + threadId + "/" + post.getId() + "/download/$1)");
	}

	@Provides
	PostRenderer blogPostRenderer(HtmlRenderer renderer, Parser parser) {
		return (threadId, post) -> {
			if (!post.getId().equals("root")) {
				return post.getContent(); // just skip rendering for comments
			}
			String content = post.getContent();
			String fixed = replaceAttachmentMedia(threadId, post);
			String rendered = renderer.render(parser.parse(fixed));
			return new BlogPostContent(content, rendered, renderPreview(fixed, renderer, parser));
		};
	}

	@Provides
	PrivilegePredicate privilegePredicate() {
		return request -> !request.getPath().startsWith("/admin") && !request.getPath().startsWith("/new");
	}

	@Provides
	@Mapped(value = "/:threadID/root/edit", method = POST)
	AsyncServlet editPost(MustacheTemplater templater, PostRenderer postRenderer) {
		return attachThreadDao().serve(request -> {
			CommDao commDao = request.getAttachment(CommDao.class);
			String threadId = request.getPathParameter("threadID");
			UserId userId = request.getAttachment(UserId.class);
			UserRole userRole = request.getAttachment(UserRole.class);
			return request.getAttachment(ThreadDao.class)
					.getPost("root")
					.then(post -> userRole.isPrivileged() || post.getAuthor().equals(userId) ?
							handleAttachments(request, "root") :
							Promise.ofException(HttpException.ofCode(403, "Not privileged")))
					.then($ -> {
						Promise<ThreadView> thread = ThreadView.root(commDao, threadId, userId, userRole, postRenderer);
						return templater.render("root_post", map("thread", thread, ".", thread.map(ThreadView::getRoot)));
					});
		});
	}

	public static final class BlogPostContent {
		private final String raw;
		private final String rendered;
		private final String renderedPreview;

		public BlogPostContent(String raw, String rendered, String renderedPreview) {
			this.raw = raw;
			this.rendered = rendered;
			this.renderedPreview = renderedPreview;
		}

		public String getRaw() {
			return raw;
		}

		public String getRendered() {
			return rendered;
		}

		public String getRenderedPreview() {
			return renderedPreview;
		}

		@Override
		public String toString() {
			return getRaw();
		}
	}
}
