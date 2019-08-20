package io.datakernel.docs.module;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.EagerSingleton;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.automation.WarmUpService;
import io.datakernel.docs.dao.MarkdownDao;
import io.datakernel.docs.render.ContentRenderer;
import io.datakernel.service.BlockingService;

import java.nio.file.Paths;

public class AutomationModule extends AbstractModule {
	private static final String MAIN = "main.html";
	private static final String NOT_FOUND_PAGE = "404.html";

	@Provides
	@EagerSingleton
	BlockingService warmUpService(Config config, @Optional MarkdownDao markdownDao, @Optional ContentRenderer contentRenderer) {
		return WarmUpService.create(contentRenderer, markdownDao,
				Paths.get(config.get("templates.path") + "/" + MAIN),
				Paths.get(config.get("templates.path") + "/" + NOT_FOUND_PAGE));
	}
}
