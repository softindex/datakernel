package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoaders.ofClassPath;
import static io.datakernel.util.CollectionUtils.list;
import static java.lang.Boolean.parseBoolean;

public final class HttpFileUploadExample extends HttpServerLauncher {
	private Path PATH = Paths.get("src/main/resources/");

	@Override
	protected void onStop() throws Exception {
		Files.delete(PATH);
	}

	@Override
	protected Collection<Module> getBusinessLogicModules() {
		return list(new AbstractModule() {
			@Provides
			@Singleton
			ExecutorService provide() {
				return Executors.newCachedThreadPool();
			}

			@Provides
			@Singleton
			AsyncServlet provide(Eventloop eventloop, ExecutorService executor) {
				return RoutingServlet.create()
						.with(GET, "/*", StaticServlet.create(eventloop, ofClassPath(executor, "static/multipart/")))
						.with(POST, "/test", request ->
								request.getFiles(name -> ChannelFileWriter.create(PATH.resolve(name)))
										.map($ -> HttpResponse.ok200().withPlainText("Upload successful")));
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpFileUploadExample();
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
