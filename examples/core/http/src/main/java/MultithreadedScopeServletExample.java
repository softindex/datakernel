import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.di.RequestScope;
import io.datakernel.http.di.ScopeServlet;
import io.datakernel.launchers.http.MultithreadedHttpServerLauncher;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;

import java.util.function.Function;

//[START EXAMPLE]
public final class MultithreadedScopeServletExample extends MultithreadedHttpServerLauncher {
	@Provides
	@Worker
	AsyncServlet servlet(Injector injector) {
		return new ScopeServlet(injector) {
			@Provides
			Function<Object[], String> template() {
				return args -> String.format("Hello from worker server %1$s\n\n%2$s", args);
			}

			@Provides
			@RequestScope
			String content(HttpRequest request, @WorkerId int workerId, Function<Object[], String> template) {
				return template.apply(new Object[]{workerId, request});
			}

			@Provides
			@RequestScope
			Promise<HttpResponse> httpResponse(String content) {
				return Promise.of(HttpResponse.ok200().withPlainText(content));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		MultithreadedScopeServletExample example = new MultithreadedScopeServletExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
