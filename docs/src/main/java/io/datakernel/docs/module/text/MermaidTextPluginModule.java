package io.datakernel.docs.module.text;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.text.MermaidTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;

public final class MermaidTextPluginModule extends AbstractModule {
	@Export
	@ProvidesIntoSet
	TextPlugin mermaidPlugin(Config config) {
		return new MermaidTextPlugin(config.get("js.path"));
	}
}
