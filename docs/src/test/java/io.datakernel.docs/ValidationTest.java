package io.datakernel.docs;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.dao.MarkdownDao;
import io.datakernel.docs.module.RenderModule;
import io.datakernel.docs.render.ContentRenderException;
import io.datakernel.docs.render.ContentRenderer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.datakernel.config.Config.ofProperties;
import static java.lang.System.getProperties;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Ignore
public final class ValidationTest {
	private static final String MAIN_PAGE = "main.html";
	private static final Logger logger = LoggerFactory.getLogger(ValidationTest.class);
	private MarkdownDao markdownDao;
	private ContentRenderer contentRenderer;
	private Path templatePath;

	@Before
	public void init() {
		Injector injector = Injector.of(
				new RenderModule()
						.rebindImport(Key.of(Config.class), Key.of(Config.class, "properties")),
				new AbstractModule() {
					@Export
					@Provides
					@Named("properties")
					Config config() {
						return ofProperties("app.properties")
								.overrideWith(ofProperties(getProperties())
										.getChild("config"))
								.with("caching.use", "true");
					}
					@Export
					@Provides
					Executor executor() {
						return newCachedThreadPool();
					}
				});
		this.markdownDao = injector.getInstance(MarkdownDao.class);
		this.contentRenderer = injector.getInstance(ContentRenderer.class);
		Config config = injector.getInstance(Key.of(Config.class, "properties"));
		this.templatePath = Paths.get(config.get("servlet.templates.path") + "/" + MAIN_PAGE);
	}

	@Test
	public void test() throws IOException {
		Map<String, List<Exception>> pathToThrowableList = new HashMap<>();
		for (String relativePath : markdownDao.indexes()) {
			try {
				contentRenderer.render(this.templatePath, relativePath);
			} catch (ContentRenderException e) {
				pathToThrowableList.put(relativePath, e.getCauseExceptionList());
			}
		}
		pathToThrowableList.forEach((path, throwableList) -> {
			logger.error(path);
			throwableList.forEach(throwable -> logger.warn(throwable.getMessage()));
		});
		Assert.assertTrue(pathToThrowableList.isEmpty());
	}
}
