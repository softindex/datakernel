import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

//[START EXAMPLE]
public final class HttpHelloWorldExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return request -> HttpResponse.ok200().withPlainText("Hello World");
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpHelloWorldExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
