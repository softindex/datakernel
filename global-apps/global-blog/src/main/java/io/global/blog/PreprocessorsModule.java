package io.global.blog;

import com.vladsch.flexmark.ext.anchorlink.AnchorLinkExtension;
import com.vladsch.flexmark.ext.jekyll.front.matter.JekyllFrontMatterExtension;
import com.vladsch.flexmark.ext.jekyll.tag.JekyllTagExtension;
import com.vladsch.flexmark.ext.media.tags.MediaTagsExtension;
import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.data.MutableDataSet;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.blog.http.view.PostView;
import io.global.blog.preprocessor.CommentsPreprocessor;
import io.global.blog.preprocessor.Preprocessor;

import static com.vladsch.flexmark.ext.anchorlink.AnchorLinkExtension.ANCHORLINKS_WRAP_TEXT;
import static com.vladsch.flexmark.html.HtmlRenderer.RENDER_HEADER_ID;
import static com.vladsch.flexmark.parser.Parser.EXTENSIONS;
import static com.vladsch.flexmark.parser.Parser.PARSER_EMULATION_PROFILE;
import static com.vladsch.flexmark.parser.ParserEmulationProfile.GITHUB_DOC;
import static io.global.blog.preprocessor.PostViewPreprocessors.*;
import static java.util.Arrays.asList;

public final class PreprocessorsModule extends AbstractModule {
	@Provides
	@Export
	@Named("postView")
	Preprocessor<PostView> postViewPreprocessor(HtmlRenderer renderer, Parser parser) {
		return replaceAttachmentLinks()
				.then(renderedContent(renderer, parser));
	}

	@Provides
	@Export
	@Named("comments")
	Preprocessor<PostView> commentsPreprocessor() {
		return new CommentsPreprocessor();
	}

	@Provides
	@Export
	@Named("threadList")
	Preprocessor<PostView> threadListPostViewPreprocessor(HtmlRenderer renderer, Parser parser) {
		return replaceAttachmentLinks()
				.then(renderedPreview(renderer, parser));
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
				JekyllTagExtension.create(),
				MediaTagsExtension.create(),
				AnchorLinkExtension.create(),
				JekyllFrontMatterExtension.create(),
				TablesExtension.create()));
		return options;
	}
}
