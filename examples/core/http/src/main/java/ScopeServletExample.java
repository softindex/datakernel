import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.di.RequestScope;
import io.datakernel.http.di.ScopeServlet;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.datakernel.promise.Promise;

import java.util.function.Function;

//[START EXAMPLE]
public final class ScopeServletExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet(Injector injector) {
		return new ScopeServlet(injector) {
			@Provides
			Function<Object[], String> template() {
				return args -> String.format("Hello world from DI Servlet\n\n%1$s", args);
			}

			@Provides
			@RequestScope
			String content(HttpRequest request, Function<Object[], String> template) {
				return template.apply(new Object[]{request});
			}

			@Provides
			@RequestScope
			Promise<HttpResponse> httpResponse(String content) {
				return Promise.of(HttpResponse.ok200().withPlainText(content));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		ScopeServletExample example = new ScopeServletExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
