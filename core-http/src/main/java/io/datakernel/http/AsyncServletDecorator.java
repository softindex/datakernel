package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.*;

import static io.datakernel.csp.ChannelConsumers.recycling;
import static io.datakernel.http.AsyncServlet.firstSuccessful;
import static java.util.Arrays.asList;

/**
 * A higher order function that allows transformations of {@link AsyncServlet} functions.
 */
public interface AsyncServletDecorator {
	@NotNull AsyncServlet serve(@NotNull AsyncServlet servlet);

	default @NotNull AsyncServlet serveFirstSuccessful(@NotNull AsyncServlet... servlets) {
		return serve(firstSuccessful(servlets));
	}

	static AsyncServletDecorator create() {
		return servlet -> servlet;
	}

	default AsyncServletDecorator then(AsyncServletDecorator next) {
		return servlet -> this.serve(next.serve(servlet));
	}

	@NotNull
	static AsyncServletDecorator combineDecorators(AsyncServletDecorator... decorators) {
		return combineDecorators(asList(decorators));
	}

	@NotNull
	static AsyncServletDecorator combineDecorators(List<AsyncServletDecorator> decorators) {
		return decorators.stream()
				.reduce(create(), AsyncServletDecorator::then);
	}

	static AsyncServletDecorator onRequest(Consumer<HttpRequest> consumer) {
		return servlet ->
				request -> {
					consumer.accept(request);
					return servlet.serve(request);
				};
	}

	static AsyncServletDecorator onResponse(Consumer<HttpResponse> consumer) {
		return servlet ->
				request -> servlet.serve(request).whenResult(consumer);
	}

	static AsyncServletDecorator onResponse(BiConsumer<HttpRequest, HttpResponse> consumer) {
		return servlet ->
				request -> servlet.serve(request)
						.whenResult(response -> consumer.accept(request, response));
	}

	static AsyncServletDecorator mapResponse(Function<HttpResponse, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serve(request)
						.map(response -> {
							HttpResponse newResponse = fn.apply(response);
							if (response != newResponse) {
								response.recycle();
							}
							return newResponse;
						});
	}

	static AsyncServletDecorator mapResponse(BiFunction<HttpRequest, HttpResponse, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serve(request)
						.map(response -> {
							HttpResponse newResponse = fn.apply(request, response);
							if (response != newResponse) {
								ChannelSupplier<ByteBuf> bodyStream = response.getBodyStream();
								if (bodyStream != null) {
									bodyStream.streamTo(recycling());
								}
								response.recycle();
							}
							return newResponse;
						});
	}

	static AsyncServletDecorator onException(BiConsumer<HttpRequest, Throwable> consumer) {
		return servlet ->
				request -> servlet.serve(request).whenException((e -> consumer.accept(request, e)));
	}

	static AsyncServletDecorator mapException(Function<Throwable, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serve(request)
						.thenEx(((response, e) -> {
							if (e == null) {
								return Promise.of(response);
							} else {
								return Promise.of(fn.apply(e));
							}
						}));
	}

	static AsyncServletDecorator mapException(BiFunction<HttpRequest, Throwable, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serve(request)
						.thenEx(((response, e) -> {
							if (e == null) {
								return Promise.of(response);
							} else {
								return Promise.of(fn.apply(request, e));
							}
						}));
	}

	static AsyncServletDecorator mapException(Predicate<Throwable> predicate, AsyncServlet fallbackServlet) {
		return servlet ->
				request -> servlet.serve(request)
						.thenEx((response, e) -> predicate.test(e) ?
								fallbackServlet.serve(request) :
								Promise.of(response, e));
	}

	static AsyncServletDecorator mapHttpException(AsyncServlet fallbackServlet) {
		return mapException(throwable -> throwable instanceof HttpException, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpException404(AsyncServlet fallbackServlet) {
		return mapException(throwable -> throwable instanceof HttpException && ((HttpException) throwable).getCode() == 404, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpException500(AsyncServlet fallbackServlet) {
		return mapException(throwable -> throwable instanceof HttpException && ((HttpException) throwable).getCode() == 500, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpClientException(AsyncServlet fallbackServlet) {
		return mapException(throwable -> {
			if (throwable instanceof HttpException) {
				int code = ((HttpException) throwable).getCode();
				return code >= 400 && code < 500;
			}
			return false;
		}, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpServerException(AsyncServlet fallbackServlet) {
		return mapException(throwable -> {
			if (throwable instanceof HttpException) {
				int code = ((HttpException) throwable).getCode();
				return code >= 500 && code < 600;
			}
			return false;
		}, fallbackServlet);
	}

	static AsyncServletDecorator mapToHttp500Exception() {
		return mapToHttpException(e -> HttpException.internalServerError500());
	}

	static AsyncServletDecorator mapToHttpException(Function<Throwable, HttpException> fn) {
		return servlet ->
				request -> servlet.serve(request)
						.thenEx(((response, e) -> {
							if (e == null) {
								return Promise.of(response);
							} else {
								if (e instanceof HttpException) return Promise.ofException(e);
								return Promise.ofException(fn.apply(e));
							}
						}));
	}

	static AsyncServletDecorator mapToHttpException(BiFunction<HttpRequest, Throwable, HttpException> fn) {
		return servlet ->
				request -> servlet.serve(request)
						.thenEx(((response, e) -> {
							if (e == null) {
								return Promise.of(response);
							} else {
								if (e instanceof HttpException) return Promise.ofException(e);
								return Promise.ofException(fn.apply(request, e));
							}
						}));
	}

	static AsyncServletDecorator catchUncheckedExceptions() {
		return servlet ->
				request -> {
					try {
						return servlet.serve(request);
					} catch (UncheckedException u) {
						return Promise.ofException(u.getCause());
					}
				};
	}

	static AsyncServletDecorator catchRuntimeExceptions() {
		return servlet ->
				request -> {
					try {
						return servlet.serve(request);
					} catch (UncheckedException u) {
						return Promise.ofException(u.getCause());
					} catch (RuntimeException e) {
						return Promise.ofException(e);
					}
				};
	}

	static AsyncServletDecorator setMaxBodySize(MemSize maxBodySize) {
		return setMaxBodySize(maxBodySize.toInt());
	}

	static AsyncServletDecorator setMaxBodySize(int maxBodySize) {
		return servlet ->
				request -> {
					request.setMaxBodySize(maxBodySize);
					return servlet.serve(request);
				};
	}

	static AsyncServletDecorator loadBody() {
		return servlet ->
				request -> request.loadBody()
						.then($ -> servlet.serve(request));
	}

	static AsyncServletDecorator loadBody(MemSize maxBodySize) {
		return servlet ->
				request -> request.loadBody(maxBodySize)
						.then($ -> servlet.serve(request));
	}

	static AsyncServletDecorator loadBody(int maxBodySize) {
		return servlet ->
				request -> request.loadBody(maxBodySize)
						.then($ -> servlet.serve(request));
	}

}
