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

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoader.ofClassPath;
import static java.lang.Thread.currentThread;

public final class FileUploadExample extends HttpServerLauncher {
	private Path path;

	@Override
	protected void onStart() throws Exception {
		path = Files.createTempDirectory("upload-example");
	}

	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.with(GET, "/*", StaticServlet.create(ofClassPath(currentThread().getContextClassLoader(), "static/multipart/"))
						.withIndexHtml())
				.with(POST, "/test", request ->
						request.getFiles(name -> ChannelFileWriter.create(path.resolve(name)))
								.map($ -> HttpResponse.ok200().withPlainText("Upload successful")));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new FileUploadExample();
		launcher.launch(args);
	}
}
