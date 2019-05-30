package io.datakernel.examples;

import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.loader.StaticLoader.ofClassPath;

//[START EXAMPLE]
public class SimplePageLauncher extends HttpServerLauncher {
	@Override
	protected Module getBusinessLogicModule() {
		return new AbstractModule() {
			@Provides
			AsyncServlet servlet() {
				return StaticServlet.create(ofClassPath("build"));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		SimplePageLauncher launcher = new SimplePageLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
