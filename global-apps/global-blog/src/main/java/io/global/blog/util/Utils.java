package io.global.blog.util;


import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.datakernel.http.decoder.DecodeErrors;
import io.datakernel.http.decoder.Decoder;
import io.datakernel.promise.Promise;
import io.global.blog.http.PublicServlet;
import io.global.blog.ot.BlogMetadata;
import io.global.mustache.MustacheTemplater;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.decoder.Decoders.ofGet;
import static io.global.comm.util.Utils.createCommRegistry;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils {
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";
	public static final StructuredCodec<Map<String, String>> PARAM_CODEC = StructuredCodecs.ofMap(STRING_CODEC, STRING_CODEC);

	public static final CodecRegistry REGISTRY = createCommRegistry()
			.with(BlogMetadata.class, registry -> tuple(BlogMetadata::new,
					BlogMetadata::getTitle, STRING_CODEC,
					BlogMetadata::getDescription, STRING_CODEC));
	public static final Decoder<Tuple2<Integer, Integer>> PAGINATION_DECODER = Decoder.of(Tuple2::new,
			ofGet("page")
					.map(Integer::valueOf, "Cannot parse page")
					.validate(value -> value > 0, "Cannot be less or equal 0"),
			ofGet("size")
					.map(Integer::valueOf, "Cannot parse size")
					.validate(value -> value >= 0, "Cannot be less 0")
	);

	public static AsyncServletDecorator renderErrors(MustacheTemplater templater) {
		return servlet ->
				request ->
						servlet.serve(request)
								.get()
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
									return templater.render(code, "error", map("code", code, "message", message));
								});
	}

	@Nullable
	public static <T> T castIfExist(Object o, Class<T> type) {
		return o == null ? null : type.isInstance(o) ? type.cast(o) : null;
	}

	public static HttpException toHttpException(DecodeErrors errors) {
		StringBuilder builder = new StringBuilder();
		errors.toMultimap()
				.forEach((field, ex) -> builder.append(field)
						.append(" -> ")
						.append(ex));
		return HttpException.ofCode(403, builder.toString());
	}

	public static Promise<Void> validate(String param, int maxLength, String paramName) {
		return validate(param, maxLength, paramName, false);
	}

	public static Promise<Void> validate(@Nullable String param, int maxLength, String paramName, boolean required) {
		if (param == null && required || (param != null && param.matches(WHITESPACE) && required)) {
			return Promise.ofException(new ParseException(PublicServlet.class, "'" + paramName + "' POST parameter is required"));
		}
		return param != null && param.length() > maxLength ?
				Promise.ofException(new ParseException(PublicServlet.class, paramName + " is too long (" + param.length() + ">" + maxLength + ")")) :
				Promise.complete();
	}
}
