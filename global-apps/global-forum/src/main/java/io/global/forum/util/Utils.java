package io.global.forum.util;

import io.datakernel.async.Promise;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.global.comm.pojo.IpRange;
import io.global.forum.http.IpBanRequest;
import io.global.forum.ot.ForumMetadata;
import io.global.mustache.MustacheTemplater;

import java.time.Instant;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.comm.util.Utils.createCommRegistry;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final CodecFactory REGISTRY = createCommRegistry()
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
}
