package io.datakernel.http.di;

import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpMethod;
import io.datakernel.http.RoutingServlet;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class RouterModule extends AbstractModule {

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	protected void configure() {
		List<Key<? extends AsyncServlet>> mappedKeys = new ArrayList<>();

		transform(0, (bindings, scope, key, binding) -> {
			if (scope.length == 0 && key.getAnnotationType() == Mapped.class && AsyncServlet.class.isAssignableFrom(key.getRawType())) {
				mappedKeys.add((Key<AsyncServlet>) (Key) key);
			}
			return binding;
		});

		bind(AsyncServlet.class)
				.annotatedWith(Router.class)
				.to(injector -> {
					RoutingServlet router = RoutingServlet.create();
					for (Key<? extends AsyncServlet> key : mappedKeys) {
						AsyncServlet servlet = injector.getInstance(key);
						Mapped mapped = (Mapped) key.getAnnotation();
						assert mapped != null;
						router.map(mapped.method().getHttpMethod(), mapped.value(), servlet);
					}
					return router;
				}, Injector.class);
	}

	@NameAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface Mapped {
		MappedHttpMethod method() default MappedHttpMethod.ALL;

		String value();
	}

	@NameAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface Router {
	}

	public enum MappedHttpMethod {
		GET, PUT, POST, HEAD, DELETE, CONNECT, OPTIONS, TRACE, PATCH,
		SEARCH, COPY, MOVE, LOCK, UNLOCK, MKCOL, PROPFIND, PROPPATCH,

		ALL;

		@Nullable
		HttpMethod getHttpMethod() {
			return this == ALL? null : HttpMethod.values()[ordinal()];
		}
	}
}
