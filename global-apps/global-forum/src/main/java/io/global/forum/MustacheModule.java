package io.global.forum;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.forum.util.MustacheTemplater;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.config.ConfigConverters.ofPath;

public final class MustacheModule extends AbstractModule {
	public static final Path DEFAULT_TEMPLATE_PATH = Paths.get("static/templates");

	@Provides
	MustacheFactory mustacheFactory(Config config) {
		Path templatesDir = config.get(ofPath(), "static.templates", DEFAULT_TEMPLATE_PATH);
		return new DefaultMustacheFactory(templatesDir.toFile());
	}

	@Export
	@Provides
	MustacheTemplater mustacheTemplater(MustacheFactory mustacheFactory) {
		Map<String, Mustache> templateCache = new HashMap<>();
		return new MustacheTemplater(filename -> templateCache.computeIfAbsent(filename, mustacheFactory::compile));
	}
}
