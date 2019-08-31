package io.global.forum;

import com.github.mustachejava.DefaultMustacheFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.global.forum.Utils.MustacheTemplater;

import java.io.File;

public final class DebugMustacheModule extends AbstractModule {

	@Provides
	MustacheTemplater mustacheTemplater(Config config) {
		String templatePath = config.get("static.templates");
		return new MustacheTemplater(filename -> new DefaultMustacheFactory(new File(templatePath)).compile(filename));
	}
}
