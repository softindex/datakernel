package io.global.forum;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.forum.Utils.MustacheSupplier;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public final class MustacheModule extends AbstractModule {

	@Provides
	MustacheFactory mustacheFactory(Config config) {
		return new DefaultMustacheFactory(new File(config.get("static.templates")));
	}

	@Export
	@Provides
	MustacheSupplier mustacheSupplier(MustacheFactory mustacheFactory) {
		Map<String, Mustache> templateCache = new HashMap<>();
		return filename -> templateCache.computeIfAbsent(filename, mustacheFactory::compile);
	}
}
