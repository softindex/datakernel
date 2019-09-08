import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.promise.Promise;

import java.util.concurrent.Executor;

import static java.util.concurrent.Executors.newCachedThreadPool;

public final class BlockingServletExample extends HttpServerLauncher {
	@Provides
	Executor executor() {
		return newCachedThreadPool();
	}

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return RoutingServlet.create()
				.map("/", request -> Promise.of(
						HttpResponse.ok200()
								.withHtml("<a href='hardWork'>Do hard work</a>")))
				.map("/hardWork", AsyncServlet.ofBlocking(executor, request -> {
					Thread.sleep(2000); //Hard work
					return HttpResponse.ok200()
							.withHtml("Hard work is done");
				}));
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new BlockingServletExample();
		launcher.launch(args);
	}
}
