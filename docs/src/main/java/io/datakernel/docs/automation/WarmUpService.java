package io.datakernel.docs.automation;

import io.datakernel.docs.dao.MarkdownDao;
import io.datakernel.docs.render.ContentRenderException;
import io.datakernel.docs.render.ContentRenderer;
import io.datakernel.service.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Service fail in case there is at least one problem, to find them should run tests before
 */
public class WarmUpService implements BlockingService {
	private static final Logger logger = LoggerFactory.getLogger(WarmUpService.class);
	private static final String EXCLUDE_ERROR_PAGE = "404";
	private final ContentRenderer contentRenderer;
	private final MarkdownDao markdownDao;
	private final Path notFoundTemplatePath;
	private final Path templatePath;

	private WarmUpService(ContentRenderer contentRenderer, MarkdownDao markdownDao, Path templatePath, Path notFoundTemplatePath) {
		this.contentRenderer = contentRenderer;
		this.templatePath = templatePath;
		this.markdownDao = markdownDao;
		this.notFoundTemplatePath = notFoundTemplatePath;
	}

	public static WarmUpService create(ContentRenderer contentRenderer, MarkdownDao markdownDao, Path templatePath, Path notFoudTemplatePath) {
		return new WarmUpService(contentRenderer, markdownDao, templatePath, notFoudTemplatePath);
	}

	@Override
	public void start() throws IOException, ContentRenderException {
		logger.info("WarmUpService is starting");
		for (String relativePath : markdownDao.indexes()) {
			if (relativePath.contains(EXCLUDE_ERROR_PAGE)) {
				contentRenderer.render(notFoundTemplatePath, relativePath);
			} else {
				contentRenderer.render(templatePath, relativePath);
			}
		}
	}

	@Override
	public void stop() {}
}
