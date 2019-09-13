package io.global.forum.util;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.global.comm.pojo.IpRange;
import io.global.forum.http.IpBanRequest;
import io.global.forum.ot.ForumMetadata;

import java.time.Instant;
import java.util.function.BiFunction;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.http.HttpHeaders.REFERER;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.comm.util.Utils.createCommRegistry;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final CodecRegistry REGISTRY = createCommRegistry()
			.with(ForumMetadata.class, registry -> tuple(ForumMetadata::new,
					ForumMetadata::getName, STRING_CODEC,
					ForumMetadata::getDescription, STRING_CODEC))
			.with(IpBanRequest.class, registry -> tuple(IpBanRequest::new,
					IpBanRequest::getRange, registry.get(IpRange.class),
					IpBanRequest::getUntil, registry.get(Instant.class),
					IpBanRequest::getDescription, STRING_CODEC));

	public static AsyncServletDecorator renderErrors(MustacheTemplater templater) {
		return servlet ->
				request ->
						servlet.serve(request)
								.thenEx((response, e) -> {
									if (e != null) {
										int code = e instanceof HttpException ? ((HttpException) e).getCode() : 500;
										return templater.render(code, "error", map("code", code, "message", e.getMessage()));
									}
									int code = response.getCode();
									if (code < 400) {
										return Promise.of(response);
									}
									String message = response.isBodyLoaded() ? response.getBody().asString(UTF_8) : "";
									return templater.render(code, "error", map("code", code, "message", message.isEmpty() ? null : message));
								});
	}

	public static HttpResponse redirectToReferer(HttpRequest request, String defaultPath) {
		String referer = request.getHeader(REFERER);
		return HttpResponse.redirect302(referer != null ? referer : defaultPath);
	}

	public static <T, E> BiFunction<T, Throwable, Promise<T>> revertIfException(AsyncSupplier<E> undo) {
		return (result, e) -> {
			if (e == null) {
				return Promise.of(result);
			}
			return undo.get()
					.thenEx(($2, e2) -> {
						if (e2 != null) {
							e.addSuppressed(e2);
						}
						return Promise.ofException(e);
					});
		};
	}
}
