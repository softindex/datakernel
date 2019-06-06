package io.datakernel.examples;

import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.logger.LoggerConfigurer;

import static io.datakernel.loader.StaticLoader.ofClassPath;

//[START EXAMPLE]
public class SimplePageLauncher extends HttpServerLauncher {
	static {
		LoggerConfigurer.enableLogging();
	}
	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath("build"))
				.withIndexHtml();
	}

	public static void main(String[] args) throws Exception {
		SimplePageLauncher launcher = new SimplePageLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
