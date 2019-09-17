import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Multibinder;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.util.Initializer;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;

public final class MultibinderExample {

	static class AnnotatedServletsProvider extends AbstractModule {

		@ProvidesIntoSet
		public Consumer<RoutingServlet> mainpage() {
			return routingServlet ->
					routingServlet.map("/",
							req -> Promise.of(HttpResponse.ok200().withPlainText("Hello from main page!")));
		}

		@ProvidesIntoSet
		public Consumer<RoutingServlet> wrongPage() {
			return routingServlet ->
					routingServlet.map("/wrong",
							req -> Promise.of(HttpResponse.notFound404()));
		}

		@ProvidesIntoSet
		public Consumer<RoutingServlet> noAuth() {
			return routingServlet ->
					routingServlet.map("/noauth",
							req -> Promise.of(HttpResponse.unauthorized401("Sorry, bro")));
		}

		@ProvidesIntoSet
		AsyncServlet primary(Set<Consumer<RoutingServlet>> initializers) {
			RoutingServlet routingServlet = RoutingServlet.create();
			initializers.forEach(el -> el.accept(routingServlet));
			return routingServlet;
		}
	}

	static class DirectServletProvider extends AbstractModule {

		@Override
		protected void configure() {
			multibind(new Key<Set<Initializer<RoutingServlet>>>() {}, Multibinder.toSet());
		}

		@Provides
		public Set<Initializer<RoutingServlet>> indexPage() {
			return singleton(routingServlet ->
					routingServlet.map("/index",
							req -> Promise.of(HttpResponse.ok200().withPlainText("Hello from index page!"))));
		}

		@Provides
		public Set<Initializer<RoutingServlet>> page404() {
			return singleton(routingServlet ->
					routingServlet.map("/notfound",
							req -> Promise.of(HttpResponse.notFound404())));
		}

		@Provides
		public Set<Initializer<RoutingServlet>> page401() {
			return singleton(routingServlet ->
					routingServlet.map("/notauth",
							req -> Promise.of(HttpResponse.unauthorized401("Sorry, bro"))));
		}

		@ProvidesIntoSet
		AsyncServlet primary2(Set<Initializer<RoutingServlet>> initializers) {
			return RoutingServlet.create().initialize(Initializer.combine(initializers));
		}
	}

	static class ServletMapProvider extends AbstractModule {

		@Override
		protected void configure() {
			multibind(new Key<Map<String, AsyncServlet>>() {}, Multibinder.toMap());
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
		AsyncServlet primary3(Map<String, AsyncServlet> initializers) {
			RoutingServlet routingServlet = RoutingServlet.create();
			initializers.forEach(routingServlet::map);
			return routingServlet;
		}
	}

	public static void main(String[] args) {
		AnnotatedServletsProvider annotatedServletsProvider = new AnnotatedServletsProvider();
		DirectServletProvider directServletProvider = new DirectServletProvider();
		ServletMapProvider mapServletProvider = new ServletMapProvider();
		Injector injector = Injector.of(annotatedServletsProvider, directServletProvider, mapServletProvider);

		String s = injector.getInstance(new Key<Set<AsyncServlet>>() {}).toString();
		System.out.println(s);
	}
}
