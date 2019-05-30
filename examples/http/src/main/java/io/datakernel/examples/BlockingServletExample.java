package io.datakernel.examples;

import io.datakernel.async.Promise;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;

public class BlockingServletExample extends HttpServerLauncher {
	@Override
	protected Module getBusinessLogicModule() {
		return new AbstractModule() {
			@Provides
			AsyncServlet servlet() {
				return RoutingServlet.create()
						.with("/", request -> Promise.of(HttpResponse.ok200()
								.withBody(wrapUtf8("<a href='hardWork'>Do hard work</a>"))))
						.with("/hardWork", AsyncServlet.ofBlocking(request -> {
							Thread.sleep(2000); //Hard work
							return HttpResponse.ok200()
									.withBody(wrapUtf8("<h1>Hard work is done<h1>"));
						}));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new BlockingServletExample();
		launcher.launch(args);
	}
}
