package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.Collection;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;

public class BlockingServletExample extends HttpServerLauncher {
	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			AsyncServlet provide() {
				return RoutingServlet.create()
						.with("/", request -> Promise.of(HttpResponse.ok200()
								.withBody(wrapUtf8("<a href='hardWork'>Do hard work</a>"))))
						.with("/hardWork", AsyncServlet.ofBlocking(request -> {
									Thread.sleep(2000); //Hard work
									return HttpResponse.ok200()
											.withBody(wrapUtf8("<h1>Hard work is done<h1>"));
								}));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new BlockingServletExample();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
