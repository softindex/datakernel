package io.global.mustache;

import com.github.mustachejava.DefaultMustacheFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.ofPath;

public final class DebugMustacheModule extends AbstractModule {
	public static final Path DEFAULT_TEMPLATE_PATH = Paths.get("static/templates");

	@Provides
	MustacheTemplater mustacheTemplater(Config config, Executor executor) {
		File templatesDir = config.get(ofPath(), "static.templates", DEFAULT_TEMPLATE_PATH).toFile();
		return new MustacheTemplater(executor, filename -> new DefaultMustacheFactory(templatesDir).compile(filename));
	}
}
