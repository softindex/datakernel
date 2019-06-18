import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.loader.StaticLoader.ofClassPath;
import static java.lang.Thread.currentThread;

//[START EXAMPLE]
public final class SimpleApplicationLauncher extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath(currentThread().getContextClassLoader(), "build"))
				.withIndexHtml();
	}

	public static void main(String[] args) throws Exception {
		SimpleApplicationLauncher launcher = new SimpleApplicationLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
