package io.datakernel.docs.module.text;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.text.GraphVizTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;

public final class GraphVizPluginModule extends AbstractModule {
	@Export
	@ProvidesIntoSet
	TextPlugin graphViz(Config config) {
		return new GraphVizTextPlugin(config.get("js.path"), config.get("full.js.path"));
	}
}
