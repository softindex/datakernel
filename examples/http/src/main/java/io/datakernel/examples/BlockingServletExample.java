package io.datakernel.examples;

import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.logger.LoggerConfigurer;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;

public class BlockingServletExample extends HttpServerLauncher {
	static {
		LoggerConfigurer.enableLogging();
	}

	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.with("/", request -> Promise.of(HttpResponse.ok200()
						.withBody(wrapUtf8("<a href='hardWork'>Do hard work</a>"))))
				.with("/hardWork", AsyncServlet.ofBlocking(request -> {
					Thread.sleep(2000); //Hard work
					return HttpResponse.ok200()
							.withBody(wrapUtf8("Hard work is done"));
				}));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new BlockingServletExample();
		launcher.launch(args);
	}
}
