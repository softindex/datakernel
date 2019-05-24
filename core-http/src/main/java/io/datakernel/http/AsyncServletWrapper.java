package io.datakernel.http;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.util.MemSize;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface AsyncServletWrapper {
	@NotNull AsyncServlet then(@NotNull AsyncServlet servlet);

	default AsyncServletWrapper wrap(AsyncServletWrapper next) {
		return servlet -> this.then(next.then(servlet));
	}

	@Nullable
	static AsyncServletWrapper of(AsyncServletWrapper... wrappersServlets) {
		return Arrays.stream(wrappersServlets)
				.reduce(servlet -> servlet, AsyncServletWrapper::wrap);
	}

	@Nullable
	static AsyncServletWrapper of(List<AsyncServletWrapper> wrappersServlets) {
		return wrappersServlets.stream()
				.reduce(servlet -> servlet, AsyncServletWrapper::wrap);
	}

	static AsyncServletWrapper process(BiConsumer<HttpRequest, HttpResponse> requestFunction) {
		return servlet ->
				request -> servlet.serve(request)
						.whenResult(response -> requestFunction.accept(request, response));
	}

	static AsyncServletWrapper process(Consumer<HttpRequest> requestFunction) {
		return servlet ->
				request -> {
					requestFunction.accept(request);
					return servlet.serve(request);
				};
	}

	static AsyncServletWrapper process(Function<HttpResponse, HttpResponse> responseFunction) {
		return servlet ->
				request -> servlet.serve(request).map(responseFunction);
	}

	static AsyncServletWrapper loadBody() {
		return servlet ->
				request -> request.loadBody()
						.then($ -> servlet.serve(request));
	}

	static AsyncServletWrapper loadBody(MemSize loadLimit) {
		return servlet ->
				request -> request.loadBody(loadLimit)
						.then($ -> servlet.serve(request));
	}

	static AsyncServletWrapper loadBody(int loadLimit) {
		return servlet ->
				request -> request.loadBody(loadLimit)
						.then($ -> servlet.serve(request));
	}

	static AsyncServletWrapper loadPostParams() {
		return servlet ->
				request -> request.loadPostParams()
						.then($ -> {
							return servlet.serve(request);
						});
	}

	static <T> AsyncServletWrapper parseToJson(StructuredCodec<T> codec) {
		return servlet ->
				request -> {
					try {
						T parsedJson = JsonUtils.fromJson(codec, request.getBody().getString(UTF_8));
						request.attach(parsedJson);
					} catch (ParseException ignore) {}// Parsed object will be null in attachments
					return servlet.serve(request);
				};
	}
}
