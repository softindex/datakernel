import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.loader.StaticLoader.ofClassPath;
import static java.lang.Thread.currentThread;

public final class StaticServletExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath(currentThread().getContextClassLoader(), "static/site"))
				.withIndexHtml();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
