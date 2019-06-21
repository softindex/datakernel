import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;

import static io.datakernel.loader.StaticLoader.ofClassPath;

public final class StaticServletExample extends HttpServerLauncher {
	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet() {
		return StaticServlet.create(ofClassPath("static/site"))
				.withIndexHtml();
	}
	//[END EXAMPLE]
	public static void main(String[] args) throws Exception {
		Launcher launcher = new StaticServletExample();
		launcher.launch(args);
	}
}
