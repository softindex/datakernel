package io.datakernel.examples;

import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoader.ofClassPath;

public final class HttpFileUploadExample extends HttpServerLauncher {
	private Path PATH = Paths.get("src/main/resources/");

	@Override
	protected void onStop() throws Exception {
		Files.delete(PATH);
	}

	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.with(GET, "/*", StaticServlet.create(ofClassPath("static/multipart/"))
						.withIndexHtml())
				.with(POST, "/test", request ->
						request.getFiles(name -> ChannelFileWriter.create(PATH.resolve(name)))
								.map($ -> HttpResponse.ok200().withPlainText("Upload successful")));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpFileUploadExample();
		launcher.launch(args);
	}
}
