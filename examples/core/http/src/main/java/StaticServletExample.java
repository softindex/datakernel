import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.datakernel.loader.StaticLoader.ofClassPath;

public final class StaticServletExample extends HttpServerLauncher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}

	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath("static/site"))
				.withIndexHtml();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
