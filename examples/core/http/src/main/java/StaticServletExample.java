import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class StaticServletExample extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return StaticServlet.ofClassPath(executor, "static/site")
				.withIndexHtml();
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
