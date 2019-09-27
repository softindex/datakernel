import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.datakernel.http.AsyncServletDecorator.logged;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class StaticServletExample extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return logged()
				.serve(StaticServlet.ofClassPath(executor, "static/site")
						.withIndexHtml());
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
