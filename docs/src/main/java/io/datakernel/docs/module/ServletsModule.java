package io.datakernel.docs.module;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.adapter.MarkdownIndexAdapter;
import io.datakernel.docs.dao.MarkdownDao;
import io.datakernel.docs.render.ContentRenderException;
import io.datakernel.docs.render.ContentRenderer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.AsyncServletDecorator.mapException;
import static io.datakernel.http.HttpResponse.ok200;

public final class ServletsModule extends AbstractModule {
	private static final String NOT_FOUND_PAGE = "404.html";
	private static final String MAIN_PAGE = "main.html";
	private static final String FAILED_URL = "404.md";
	private static final CharSequence SITE_URL = "https://datakernel.io/docs";

	@Override
	protected void configure() {
		install(new RenderModule());

		// reexport stuff from render module
		bind(MarkdownDao.class).export();
		bind(ContentRenderer.class).export();

		bind(Config.class).named("servlet").to(conf -> conf.getChild("servlet"), Config.class);
	}

	@Provides
	StaticLoader staticLoader(@Named("servlet") Config config, Executor executor) {
		return StaticLoader.ofClassPath(executor, config.get("templates.path"));
	}

	@Export
	@Provides
	AsyncServlet rootServlet(@Named("main") AsyncServlet mainServlet,
							 @Named("fail") AsyncServlet failServlet,
							 @Named("sitemap") AsyncServlet sitemapServlet,
							 @Named("static") AsyncServlet staticServlet) {
		return mapException(Objects::nonNull, failServlet)
				.serve(RoutingServlet.create()
						.map("/*", mainServlet)
						.map("/sitemap", sitemapServlet)
						.map("/static/*", staticServlet));
	}

	@Provides
	@Named("static")
	AsyncServlet staticServlet(@Named("servlet") Config config, Executor executor) {
		return StaticServlet.ofPath(executor, config.get(ofPath(), "static.path"));
	}

	@Provides
	@Named("fail")
	AsyncServlet failServlet(@Named("servlet") Config config, ContentRenderer contentRenderer, MarkdownIndexAdapter<String> indexAdapter) {
		Path path = Paths.get(config.get("templates.path") + "/" + NOT_FOUND_PAGE);
		String resolvedFailedUrl = indexAdapter.resolve(FAILED_URL);
		return request -> {
			try {
				ByteBuf renderedContent = contentRenderer.render(path, resolvedFailedUrl);
				return Promise.of(ok200().withBody(renderedContent));
			} catch (IOException | ContentRenderException e) {
				return Promise.of(HttpResponse.notFound404());
			}
		};
	}

	@Provides
	@Named("main")
	AsyncServlet mainServlet(@Named("servlet") Config config, ContentRenderer contentRenderer, MarkdownIndexAdapter<String> indexAdapter) {
		Path templatePath = Paths.get(config.get("templates.path") + "/" + MAIN_PAGE);
		return request -> {
			try {
				String resolvedRelativePath = indexAdapter.resolve(request.getPath());
				ByteBuf renderedContent = contentRenderer.render(templatePath, resolvedRelativePath);
				return Promise.of(ok200().withBody(renderedContent));
			} catch (IOException | ContentRenderException e) {
				return Promise.ofException(e);
			}
		};
	}

	@Provides
	@Named("sitemap")
	AsyncServlet sitemapServlet(MarkdownDao markdownDao, DocumentBuilder documentBuilder, Transformer transformer) throws TransformerException {
		Document document = documentBuilder.newDocument();
		Element urlset = document.createElement("urlset");
		urlset.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
		urlset.setAttribute("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9");
		urlset.setAttribute("xsi:schemaLocation",
				"http://www.sitemaps.org/schemas/sitemap/0.9 " +
						"http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd");
		markdownDao.indexes().forEach(index -> {
			Element url = document.createElement("url");
			Element location = document.createElement("loc");
			location.appendChild(document.createTextNode(index
					.replace("components/markdown", SITE_URL)
					.replaceFirst("\\.md", "\\.html")));
			url.appendChild(location);
			urlset.appendChild(url);
		});
		document.appendChild(urlset);
		DOMSource source = new DOMSource(document);
		StringWriter stringWriter = new StringWriter();
		transformer.transform(source, new StreamResult(stringWriter));
			return request -> Promise.of(ok200()
				.withBody(stringWriter.toString().getBytes()));
	}
}
