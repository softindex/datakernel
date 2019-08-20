package io.datakernel.docs.module.props;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.props.PropertiesPlugin;
import io.datakernel.docs.plugin.props.WebContextPropertiesPlugin;

import static io.datakernel.config.Config.ofProperties;

public final class WebContextPropertiesPluginModule extends AbstractModule {
	@Provides
	Config webConfig() {
		return ofProperties("web-context.properties");
	}

	@Export
	@ProvidesIntoSet
	PropertiesPlugin<?> webContextProperties(Config webConfig) {
		return WebContextPropertiesPlugin.create(webConfig.toMap());
	}
}
