package io.datakernel.docs.module.text;

import com.vladsch.flexmark.ast.Code;
import com.vladsch.flexmark.ext.anchorlink.AnchorLinkExtension;
import com.vladsch.flexmark.ext.jekyll.front.matter.JekyllFrontMatterExtension;
import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.AttributeProvider;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.html.IndependentAttributeProviderFactory;
import com.vladsch.flexmark.html.renderer.LinkResolverContext;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.data.MutableDataSet;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.text.MarkdownTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;

import static com.vladsch.flexmark.ext.anchorlink.AnchorLinkExtension.ANCHORLINKS_WRAP_TEXT;
import static com.vladsch.flexmark.html.HtmlRenderer.RENDER_HEADER_ID;
import static com.vladsch.flexmark.parser.Parser.EXTENSIONS;
import static com.vladsch.flexmark.parser.Parser.PARSER_EMULATION_PROFILE;
import static com.vladsch.flexmark.parser.ParserEmulationProfile.GITHUB_DOC;
import static java.util.Arrays.asList;

public class MarkdownTextPluginModule extends AbstractModule {
	@Provides
	HtmlRenderer htmlRenderer(MutableDataSet options) {
		return HtmlRenderer.builder(options)
				.attributeProviderFactory(new IndependentAttributeProviderFactory() {
					@Override
					public AttributeProvider apply(LinkResolverContext linkResolverContext) {
						return (node, attributablePart, attributes) -> {
							if (node instanceof Code) {
								attributes.replaceValue("class", "highlighter-rouge");
							}
						};
					}
				}).build();
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
				AnchorLinkExtension.create(),
				JekyllFrontMatterExtension.create(),
				TablesExtension.create()));
		return options;
	}

	@Export
	@Provides
	TextPlugin markdownTextPlugin(Parser parser, HtmlRenderer renderer) {
		return new MarkdownTextPlugin(renderer, parser);
	}
}
