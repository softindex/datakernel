import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.datakernel.loader.StaticLoader.ofClassPath;

//[START EXAMPLE]
public final class SimpleApplicationLauncher extends HttpServerLauncher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}
	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath("build"))
				.withIndexHtml();
	}

	public static void main(String[] args) throws Exception {
		SimpleApplicationLauncher launcher = new SimpleApplicationLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
