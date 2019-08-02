import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

//[START EXAMPLE]
public final class SimpleApplicationLauncher extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	@Provides
	AsyncServlet servlet(Executor executor) {
		return StaticServlet.ofClassPath(executor, "build")
				.withIndexHtml();
	}

	public static void main(String[] args) throws Exception {
		SimpleApplicationLauncher launcher = new SimpleApplicationLauncher();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
