import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.promise.Promise;

import static io.datakernel.http.HttpResponse.ok200;

//[START EXAMPLE]
public final class HttpHelloWorldExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return request -> Promise.of(ok200().withPlainText("Hello World"));
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpHelloWorldExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
