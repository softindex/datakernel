package io.datakernel.http.di;

import io.datakernel.async.Promise;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.core.Scope;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

import static java.util.Arrays.asList;

public class ScopeServlet implements AsyncServlet {
	public static final Scope REQUEST_SCOPE = Scope.of(RequestScope.class);

	public static final Key<HttpRequest> HTTP_REQUEST_KEY = new Key<HttpRequest>() {};
	public static final Key<Promise<HttpResponse>> HTTP_RESPONSE_KEY = new Key<Promise<HttpResponse>>() {};

	private final Injector injector;

	protected ScopeServlet(Injector injector, Module... modules) {
		this(injector, asList(modules));
	}

	protected ScopeServlet(Injector injector, Collection<Module> modules) {
		this.injector = Injector.of(injector,
				Modules.combine(modules),
				getModule(),
				new AbstractModule() {
					@Override
					protected void configure() {
						// so anonymous servlet subclasses could use the DSL
						scan(ScopeServlet.this);
						// dummy binding to be replaced by subInjector.putInstance
						bind(HTTP_REQUEST_KEY).in(REQUEST_SCOPE).to(() -> {
							throw new AssertionError();
						});
						// make sure that response is provided or generated in request scope
						bind(HTTP_RESPONSE_KEY).in(REQUEST_SCOPE);
					}
				}
		);
	}

	protected Module getModule() {
		return Module.create().
				install(PromiseGeneratorModule.create());
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		Injector subInjector = injector.enterScope(REQUEST_SCOPE);
		subInjector.putInstance(HTTP_REQUEST_KEY, request);
		return subInjector.getInstance(HTTP_RESPONSE_KEY);
	}
}
