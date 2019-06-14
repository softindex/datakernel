import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.loader.StaticLoader.ofClassPath;

public final class FileUploadExample extends HttpServerLauncher {
	private final Path path;
	{
		try {
			path = Files.createTempDirectory("upload-example");
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	protected void onStop() throws Exception {
		Files.delete(path);
	}

	@Provides
	AsyncServlet servlet() {
		return RoutingServlet.create()
				.with(GET, "/*", StaticServlet.create(ofClassPath("static/multipart/"))
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
