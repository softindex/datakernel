package io.datakernel.docs.module;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.adapter.MarkdownIndexAdapter;
import io.datakernel.docs.adapter.MarkdownIndexHttpRequestFilePathAdapter;
import io.datakernel.docs.dao.FileMarkdownDao;
import io.datakernel.docs.dao.MarkdownDao;
import io.datakernel.docs.module.props.NavbarPropertiesPluginModule;
import io.datakernel.docs.module.props.WebContextPropertiesPluginModule;
import io.datakernel.docs.module.text.*;
import io.datakernel.docs.plugin.TextEngineRenderer;
import io.datakernel.docs.plugin.props.PropertiesPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;
import io.datakernel.docs.render.CachedContentRenderer;
import io.datakernel.docs.render.ContentRenderer;
import io.datakernel.docs.render.ContentRendererImpl;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.datakernel.config.ConfigConverters.*;

public final class RenderModule extends AbstractModule {
	private static final String INDEX = "index.md";

	@Override
	protected void configure() {
		install(new NavbarPropertiesPluginModule().rebindImport(Key.of(Config.class), Binding.to(conf -> conf.getChild("propertiesPlugin"), Config.class)));
		install(new WebContextPropertiesPluginModule());

		install(new GithubIncludeClassComponentsTextPluginModule().rebindImport(Key.of(Config.class), Binding.to(conf -> conf.getChild("plugin"), Config.class)));
		install(new ProjectStructureTextPluginModule().rebindImport(Key.of(Config.class), Binding.to(conf -> conf.getChild("plugin"), Config.class)));
		install(new HtmlIncludeTextPluginModule().rebindImport(Key.of(Config.class), Binding.to(conf -> conf.getChild("plugin"), Config.class)));
		install(new MermaidTextPluginModule().rebindImport(Key.of(Config.class), Binding.to(conf -> conf.getChild("mermaid"), Config.class)));
		install(new LinkTextPluginModule().rebindImport(Key.of(Config.class), Binding.to(conf -> conf.getChild("plugin"), Config.class)));

		install(new PropertiesTextPluginModule());
		install(new MarkdownTextPluginModule());
		install(new HighlightPluginModule());

		bind(Config.class).named("propertiesPlugin").to(conf -> conf.getChild("propertiesPlugin"), Config.class);
		bind(Config.class).named("render").to(conf -> conf.getChild("render"), Config.class);
	}

	@Export
	@Provides
	MarkdownIndexAdapter<String> markdownIndexAdapter(@Named("render") Config config) {
		return MarkdownIndexHttpRequestFilePathAdapter.create(config.get(ofPath(), "markdown.path"));
	}

	@Export
	@Provides
	ContentRenderer cachedPageRenderer(@Named("main-renderer") ContentRenderer contentRenderer, Config config) {
		if (config.get(ofBoolean(), "caching.use", false)) {
			return CachedContentRenderer.create(contentRenderer);
		} else {
			return contentRenderer;
		}
	}

	@Export
	@Provides
	Transformer transformer() throws TransformerConfigurationException {
		return TransformerFactory.newInstance().newTransformer();
	}

	@Export
	@Provides
	DocumentBuilder documentBuilder() throws ParserConfigurationException {
		return DocumentBuilderFactory.newInstance().newDocumentBuilder();
	}

	@Provides
	@Named("main-renderer")
	ContentRenderer pageRenderer(@Named("render") Config renderConfig,
								 @Named("propertiesPlugin") Config propertiesPluginConfig,
								 MarkdownDao markdownDao,
								 MustacheFactory mustache,
								 TextEngineRenderer textEngineRenderer,
								 MarkdownIndexAdapter<String> indexAdapter,
								 Set<PropertiesPlugin<?>> propertiesPlugins) {
		String mustacheIncludePath = renderConfig.get("mustache.includes.path");
		List<String> sectors = propertiesPluginConfig.get(ofList(ofString()), "sectors");
		return ContentRendererImpl.create(markdownDao, textEngineRenderer, mustache, mustacheIncludePath, new HashSet<>(sectors))
				.withPropertiesPlugins(propertiesPlugins)
				.withIndexPage(indexAdapter.resolve(INDEX));
	}

	@Export
	@Provides
	MarkdownDao pageDao(@Named("render") Config config) {
		return FileMarkdownDao.create(config.get(ofPath(), "markdown.path"));
	}

	@Export
	@Provides
	TextEngineRenderer textEngineRenderer(Set<TextPlugin> textPluginList, TextPlugin markdownPlugin) {
		return TextEngineRenderer.create()
				.withTextPlugins(textPluginList)
				.withPlainTextPlugin(markdownPlugin);
	}

	@Provides
	MustacheFactory mustache() {
		return new DefaultMustacheFactory();
	}
}
