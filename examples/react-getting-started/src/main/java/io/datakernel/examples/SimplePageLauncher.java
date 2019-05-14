package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.Collection;

import static io.datakernel.loader.StaticLoaders.ofClassPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class SimplePageLauncher extends HttpServerLauncher {
	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			AsyncServlet staticServlet(Eventloop eventloop) {
				return StaticServlet.create(eventloop, ofClassPath(newCachedThreadPool(),"build"));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		SimplePageLauncher launcher = new SimplePageLauncher();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
