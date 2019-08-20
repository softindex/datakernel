package io.datakernel.docs.module.text;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.text.PropertiesTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;

import static io.datakernel.config.Config.ofProperties;

public final class PropertiesTextPluginModule extends AbstractModule {
	@Provides
	Config webConfig() {
		return ofProperties("web-context.properties");
	}

	@Export
	@ProvidesIntoSet
	TextPlugin propertiesPlugin(Config webConfig) {
		return new PropertiesTextPlugin(webConfig.toMap());
	}
}
