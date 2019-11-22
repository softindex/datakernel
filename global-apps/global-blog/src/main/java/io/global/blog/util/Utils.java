package io.global.blog.util;

import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.datakernel.promise.Promise;
import io.global.blog.ot.BlogMetadata;
import io.global.mustache.MustacheTemplater;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.global.Utils.isGzipAccepted;
import static io.global.comm.util.Utils.createCommRegistry;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final CodecRegistry REGISTRY = createCommRegistry()
			.with(BlogMetadata.class, registry -> tuple(BlogMetadata::new,
					BlogMetadata::getName, STRING_CODEC.nullable(),
					BlogMetadata::getDescription, STRING_CODEC.nullable()));

	public static AsyncServletDecorator renderErrors(MustacheTemplater templater) {
		return servlet ->
				request ->
						servlet.serve(request)
								.get()
								.thenEx((response, e) -> {
									if (e != null) {
										int code = e instanceof HttpException ? ((HttpException) e).getCode() : 500;
										return templater.render(code, "error", map("code", code, "message", e.getMessage()), isGzipAccepted(request));
									}
									int code = response.getCode();
									if (code < 400) {
										return Promise.of(response);
									}
									String message = response.isBodyLoaded() ? response.getBody().asString(UTF_8) : "";
									return templater.render(code, "error", map("code", code, "message", message.isEmpty() ? null : message), isGzipAccepted(request));
								});
	}

	public static <T> T castIfExist(Object o, Class<T> type) {
		return o == null ? null : type.isInstance(o) ? type.cast(o) : null;
	}
}
