package io.datakernel.docs.module.text;

import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.docs.plugin.text.HighlightTextPlugin;
import io.datakernel.docs.plugin.text.TextPlugin;
import org.python.util.PythonInterpreter;

public class HighlightPluginModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(PythonInterpreter.class).to(PythonInterpreter::new);
	}

	@Export
	@ProvidesIntoSet
	TextPlugin highlightTextPlugin(PythonInterpreter pythonInterpreter) {
		return new HighlightTextPlugin(pythonInterpreter);
	}
}
