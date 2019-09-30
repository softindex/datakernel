package io.global.forum.util;

import io.datakernel.async.Promise;
import io.datakernel.codec.registry.CodecRegistry;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.global.comm.pojo.IpRange;
import io.global.forum.http.IpBanRequest;
import io.global.forum.ot.ForumMetadata;
import io.global.mustache.MustacheTemplater;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.comm.util.Utils.createCommRegistry;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final CodecRegistry REGISTRY = createCommRegistry()
			.with(ForumMetadata.class, registry -> tuple(ForumMetadata::new,
					ForumMetadata::getTitle, STRING_CODEC,
					ForumMetadata::getDescription, STRING_CODEC))
			.with(IpBanRequest.class, registry -> tuple(IpBanRequest::new,
					IpBanRequest::getRange, registry.get(IpRange.class),
					IpBanRequest::getUntil, registry.get(Instant.class),
					IpBanRequest::getDescription, STRING_CODEC));

	public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss/dd.MM.yyyy");
	private static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";

	public static String formatInstant(@Nullable Instant timestamp) {
		return timestamp == null ? "" : timestamp.atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}

	public static String formatInstant(long timestamp) {
		return timestamp == -1 ? "" : Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).format(DATE_TIME_FORMAT);
	}

	public static AsyncServletDecorator renderErrors(MustacheTemplater templater) {
		return servlet ->
				request ->
						servlet.serve(request)
								.thenEx((response, e) -> {
									if (e != null) {
										int code;
										if (e instanceof HttpException) {
											code = ((HttpException) e).getCode();
										} else if (e instanceof ParseException) {
											code = 400;
										} else {
											code = 500;
										}
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

	@Contract("_, _, _, true -> !null")
	@Nullable
	private static String getPostParameterImpl(Map<String, String> postParameters, String name, int maxLength, boolean required) throws ParseException {
		String parameter = postParameters.get(name);
		if (parameter == null || parameter.matches(WHITESPACE)) {
			if (required) {
				throw new ParseException(Utils.class, "'" + name + "' POST parameter is required");
			}
			return null;
		}
		String trimmed = parameter.trim();
		if (trimmed.length() > maxLength) {
			throw new ParseException(Utils.class, "'" + name + "' POST parameter is too long (" + trimmed.length() + ">" + maxLength + ")");
		}
		return trimmed;
	}

	public static String getPostParameter(Map<String, String> postParameters, String name, int maxLength) throws ParseException {
		return getPostParameterImpl(postParameters, name, maxLength, true);
	}

	public static String getOptionalPostParameter(Map<String, String> postParameters, String name, int maxLength) throws ParseException {
		return getPostParameterImpl(postParameters, name, maxLength, false);
	}

	public static String getRequiredPostParameter(HttpRequest request, String name) throws ParseException {
		String parameter = request.getPostParameter(name);
		if (parameter == null) {
			throw new ParseException(Utils.class, "'" + name + "' POST parameter is required");
		}
		return parameter;
	}

	private static byte parseByte(HttpRequest request, String name) throws ParseException {
		String parameter = request.getPostParameter(name);
		if (parameter == null) {
			throw new ParseException("'" + name + "' POST parameter is required");
		}
		try {
			short s = Short.parseShort(parameter);
			if (s < 0 || s > 255) {
				throw new ParseException("POST parameter '" + name + "' parsing error: byte not in range 0-255");
			}
			return (byte) (s & 0xFF);
		} catch (NumberFormatException e) {
			throw new ParseException(e.getMessage());
		}
	}

	public static byte[] parse4Bytes(HttpRequest request, String prefix) throws ParseException {
		return new byte[]{
				parseByte(request, prefix + "_0"),
				parseByte(request, prefix + "_1"),
				parseByte(request, prefix + "_2"),
				parseByte(request, prefix + "_3")
		};
	}
}
