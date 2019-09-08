import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.promise.Promise;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;

public final class MultibinderExample {
	//[START REGION_1]
	static class ServletMapsModule extends AbstractModule {
		@Override
		protected void configure() {
			multibindToMap(String.class, AsyncServlet.class);
		}

		@Provides
		public Map<String, AsyncServlet> firstPage() {
			return singletonMap("/first",
					req -> Promise.of(HttpResponse.ok200().withPlainText("Hello from first page!")));
		}

		@Provides
		public Map<String, AsyncServlet> lastPage() {
			return singletonMap("/last",
					req -> Promise.of(HttpResponse.ok200().withPlainText("Hello from last page!")));
		}

		@ProvidesIntoSet
		AsyncServlet primary(Map<String, AsyncServlet> initializers) {
			RoutingServlet routingServlet = RoutingServlet.create();
			initializers.forEach(routingServlet::map);
			return routingServlet;
		}
	}
	//[END REGION_1]

	//[START REGION_2]
	static class ServletInitializersModule extends AbstractModule {
		@ProvidesIntoSet
		public Consumer<RoutingServlet> firstPage() {
			return routingServlet ->
					routingServlet.map("/first",
							req -> Promise.of(HttpResponse.ok200().withPlainText("Hello from first page!")));
		}

		@ProvidesIntoSet
		public Consumer<RoutingServlet> lastPage() {
			return routingServlet ->
					routingServlet.map("/last",
							req -> Promise.of(HttpResponse.ok200().withPlainText("Hello from last page!")));
		}

		@ProvidesIntoSet
		AsyncServlet primary(Set<Consumer<RoutingServlet>> initializers) {
			RoutingServlet routingServlet = RoutingServlet.create();
			initializers.forEach(initializer -> initializer.accept(routingServlet));
			return routingServlet;
		}
	}
	//[END REGION_2]

	//[START REGION_4]
	public static void main(String[] args) {
		Injector injector = Injector.of(new ServletMapsModule(), new ServletInitializersModule());

		String s = injector.getInstance(new Key<Set<AsyncServlet>>() {}).toString();
		System.out.println(s);
	}
	//[END REGION_4]
}
