package io.global.mustache;

import com.github.mustachejava.DefaultMustacheFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.datakernel.config.ConfigConverters.ofPath;

public final class DebugMustacheModule extends AbstractModule {
	public static final Path DEFAULT_TEMPLATE_PATH = Paths.get("static/templates");

	@Provides
	MustacheTemplater mustacheTemplater(Config config) {
		File templatesDir = config.get(ofPath(), "static.templates", DEFAULT_TEMPLATE_PATH).toFile();
		return new MustacheTemplater(filename -> new DefaultMustacheFactory(templatesDir).compile(filename));
	}
}
