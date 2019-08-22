package io.datakernel.docs.module.text;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.text.LinkTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;

public final class LinkTextPluginModule extends AbstractModule {

	@Export
	@ProvidesIntoSet
	TextPlugin linkTextPlugin(Config config) {
		return new LinkTextPlugin(config.get("github.url"));
	}
}
