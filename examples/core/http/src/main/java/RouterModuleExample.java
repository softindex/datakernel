import io.datakernel.di.Injector;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.di.RouterModule;
import io.datakernel.http.di.RouterModule.Mapped;
import io.datakernel.http.di.RouterModule.Router;
import io.datakernel.launchers.http.HttpServerLauncher;

//[START EXAMPLE]
public final class RouterModuleExample extends HttpServerLauncher {

	@Override
	protected Module getBusinessLogicModule() {
		return Modules.combine(new RouterModule());
	}

	@Provides
	AsyncServlet servlet(@Router AsyncServlet router) {
		return router;
	}

	@Provides
	@Mapped("/")
	AsyncServlet main() {
		return request -> HttpResponse.ok200().withPlainText("hello world");
	}

	@Provides
	@Mapped("/test1")
	AsyncServlet test1() {
		return request -> HttpResponse.ok200().withPlainText("this is test 1");
	}

	@Provides
	@Mapped("/*")
	AsyncServlet others() {
		return request -> HttpResponse.ok200().withPlainText("this is the fallback: " + request.getRelativePath());
	}

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		RouterModuleExample example = new RouterModuleExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
