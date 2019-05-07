package io.datakernel.http;

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public final class AsyncServlets {
	private AsyncServlets() {
		throw new AssertionError("nope.");
	}

	public static RoutingServlet withPrefix(String prefix, AsyncServlet servlet) {
		return RoutingServlet.create().with(prefix + "/*", servlet);
	}

	public static AsyncServlet chain(List<AsyncServlet> servlets, AsyncPredicate<MaterializedPromise<HttpResponse>> predicate) {
		assert !servlets.isEmpty() : "Cannot create servlet chain from empty list";

		Iterator<AsyncServlet> iter = servlets.iterator();
		return request -> Promises.until(() -> Promise.of(iter.next().serve(request).materialize()), predicate).then(Function.identity());
	}

	public static RoutingServlet mapAll(RoutingServlet router, UnaryOperator<AsyncServlet> leafFunction) {
		RoutingServlet copy = RoutingServlet.create();
		router.walk((method, path, servlet) -> copy.with(method, path, leafFunction.apply(servlet)));
		return copy;
	}
}
