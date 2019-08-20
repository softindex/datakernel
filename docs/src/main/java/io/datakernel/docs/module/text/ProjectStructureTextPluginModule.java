package io.datakernel.docs.module.text;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.text.ProjectStructureTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;

import java.nio.file.Path;

import static io.datakernel.config.ConfigConverters.ofPath;

public final class ProjectStructureTextPluginModule extends AbstractModule {
	@Export
	@ProvidesIntoSet
	TextPlugin projectStructureTextPlugin(Config config) {
		Path projectSourcePath = config.get(ofPath(), "projectSourceFile.path");
		return new ProjectStructureTextPlugin(projectSourcePath, config.get("github.url"), projectSourcePath.toString());
	}
}
