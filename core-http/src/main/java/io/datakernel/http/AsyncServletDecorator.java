package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.UncheckedException;
import io.datakernel.util.MemSize;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.datakernel.csp.ChannelConsumers.recycling;
import static java.util.Arrays.asList;

public interface AsyncServletDecorator {
	@NotNull AsyncServlet serve(@NotNull AsyncServlet servlet);

	default AsyncServletDecorator combine(AsyncServletDecorator next) {
		return servlet -> this.serve(next.serve(servlet));
	}

	static AsyncServletDecorator identity() {
		return servlet -> servlet;
	}

	@NotNull
	static AsyncServletDecorator combineDecorators(AsyncServletDecorator... decorators) {
		return combineDecorators(asList(decorators));
	}

	@NotNull
	static AsyncServletDecorator combineDecorators(List<AsyncServletDecorator> decorators) {
		return decorators.stream()
				.reduce(identity(), AsyncServletDecorator::combine);
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
								ChannelSupplier<ByteBuf> bodyStream = response.getBodyStream();
								if (bodyStream != null) {
									bodyStream.streamTo(recycling());
								}
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
