import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MultipartParser.MultipartDataHandler;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class FileUploadExample extends HttpServerLauncher {
	private Path path;

	@Override
	protected void onInit(Injector injector) throws Exception {
		path = Files.createTempDirectory("upload-example");
	}

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return RoutingServlet.create()
				.map(GET, "/*", StaticServlet.ofClassPath(executor, "static/multipart/")
						.withIndexHtml())
				.map(POST, "/test", request ->
						request.handleMultipart(MultipartDataHandler.file((fileName) -> ChannelFileWriter.open(executor, path.resolve(fileName))))
								.map($ -> HttpResponse.ok200().withPlainText("Upload successful")));
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		Launcher launcher = new FileUploadExample();
		launcher.launch(args);
	}
}
