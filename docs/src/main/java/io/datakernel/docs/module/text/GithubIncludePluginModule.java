package io.datakernel.docs.module.text;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.dao.FileResourceDao;
import io.datakernel.docs.dao.ResourceDao;
import io.datakernel.docs.plugin.text.GithubIncludeTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;

import java.io.FileNotFoundException;

import static io.datakernel.config.ConfigConverters.ofPath;

public final class GithubIncludePluginModule extends AbstractModule {
	@ProvidesIntoSet
	@Export
	TextPlugin bindIntoSet(ResourceDao resourceDao) {
		return new GithubIncludeTextPlugin(resourceDao);
	}

	@Provides
	ResourceDao projectSourceDao(Config config) throws FileNotFoundException {
		return FileResourceDao.create(config.get(ofPath(), "projectSourceFile.path"));
	}
}
