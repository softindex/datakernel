import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.datakernel.loader.StaticLoader.ofClassPath;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class StaticServletExample extends HttpServerLauncher {
	//[START EXAMPLE]
	@Provides
	Executor executor() {
		return newCachedThreadPool();
	}
	@Provides
	AsyncServlet servlet(Executor executor) {
		return StaticServlet.create(ofClassPath(executor, "static/site"))
				.withIndexHtml();
	}
	//[END EXAMPLE]
	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
