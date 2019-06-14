import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.datakernel.http.HttpResponse.ok200;

public final class HttpHelloWorldExample extends HttpServerLauncher {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
	}
	@Provides
	AsyncServlet servlet() {
		return request -> Promise.of(ok200().withPlainText("Hello World"));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpHelloWorldExample();
		launcher.launch(args);
	}
}
