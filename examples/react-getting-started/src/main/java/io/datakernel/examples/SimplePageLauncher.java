package io.datakernel.examples;

import io.datakernel.di.module.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.loader.StaticLoader.ofClassPath;

//[START EXAMPLE]
public class SimplePageLauncher extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath("build"))
				.withMappingEmptyTo("index.html");
	}

	public static void main(String[] args) throws Exception {
		SimplePageLauncher launcher = new SimplePageLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
