import io.datakernel.di.Injector;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.di.RequestScope;
import io.datakernel.http.di.ScopeServlet;
import io.datakernel.launchers.http.MultithreadedHttpServerLauncher;
import io.datakernel.promise.Promise;
import io.datakernel.worker.annotation.Worker;
import io.datakernel.worker.annotation.WorkerId;

import java.util.function.Function;

//[START EXAMPLE]
public final class MultithreadedScopeServletExample extends MultithreadedHttpServerLauncher {
	@Provides
	String string() {
		return "root string";
	}

	@Provides
	@Worker
	AsyncServlet servlet(@Named("1") AsyncServlet servlet1, @Named("2") AsyncServlet servlet2) {
		return RoutingServlet.create()
				.map("/", request -> HttpResponse.ok200().withHtml("<a href=\"/first\">first</a><br><a href=\"/second\">second</a>"))
				.map("/first", servlet1)
				.map("/second", servlet2);
	}

	@Provides
	@Worker
	@Named("1")
	AsyncServlet servlet1(Injector injector) {
		return new ScopeServlet(injector) {
			@Provides
			Function<Object[], String> template(String rootString) {
				return args -> String.format(rootString + "\nHello1 from worker server %1$s\n\n%2$s", args);
			}

			@Provides
			@RequestScope
			String content(HttpRequest request, @WorkerId int workerId, Function<Object[], String> template) {
				return template.apply(new Object[]{workerId, request});
			}

			//[START REGION_1]
			@Provides
			@RequestScope
			Promise<HttpResponse> httpResponse(String content) {
				return Promise.of(HttpResponse.ok200().withPlainText(content));
			}
			//[END REGION_1]
		};
	}

	@Provides
	@Worker
	@Named("2")
	AsyncServlet servlet2(Injector injector) {
		return new ScopeServlet(injector) {
			@Provides
			Function<Object[], String> template(String rootString) {
				return args -> String.format(rootString + "\nHello2 from worker server %1$s\n\n%2$s", args);
			}

			@Provides
			@RequestScope
			String content(HttpRequest request, @WorkerId int workerId, Function<Object[], String> template) {
				return template.apply(new Object[]{workerId, request});
			}

			//[START REGION_2]
			@Provides
			@RequestScope
			HttpResponse httpResponse(String content) {
				return HttpResponse.ok200().withPlainText(content);
			}
			//[END REGION_2]
		};
	}

	public static void main(String[] args) throws Exception {
		MultithreadedScopeServletExample example = new MultithreadedScopeServletExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
