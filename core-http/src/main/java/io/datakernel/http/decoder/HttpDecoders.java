package io.datakernel.http.decoder;

import io.datakernel.functional.Either;
import io.datakernel.http.HttpRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class contains some common primitive {@link HttpDecoder HttpDecoders}, that
 * can be combined to form complex ones.
 */
@SuppressWarnings("RedundantCast")
public final class HttpDecoders {
	public static final String REQUIRED_GET_PARAM = "Required GET param: %1";
	public static final String REQUIRED_POST_PARAM = "Required POST param: %1";
	public static final String REQUIRED_PATH_PARAM = "Required path param";
	public static final String REQUIRED_COOKIE = "Required cookie: %1";

	private static <T> HttpDecoder<T> ofParamEx(String paramName,
													@NotNull HttpMapper<String, T> fn,
													@NotNull BiFunction<HttpRequest, String, String> paramSupplier,
			String message) {
		return new AbstractHttpDecoder<T>(paramName) {
			@Override
			public Either<T, HttpDecodeErrors> decode(@NotNull HttpRequest request) {
				String str = paramSupplier.apply(request, paramName);
				return str != null ?
						fn.map(str)
								.mapRight(HttpDecodeErrors::of) :
						Either.right(HttpDecodeErrors.of(message, paramName));
			}
		};
	}

	private static <T> HttpDecoder<T> ofParamEx(String paramName,
													@NotNull HttpMapper<String, T> fn,
													@NotNull BiFunction<HttpRequest, String, String> paramSupplier,
													@Nullable T defaultValue) {
		return new AbstractHttpDecoder<T>(paramName) {
			@Override
			public Either<T, HttpDecodeErrors> decode(@NotNull HttpRequest request) {
				String str = paramSupplier.apply(request, paramName);
				return str != null ?
						fn.map(str)
								.mapRight(HttpDecodeErrors::of) :
						Either.left(defaultValue);
			}
		};
	}

	public static HttpDecoder<String> ofGet(String paramName) {
		return ofGetEx(paramName, (HttpMapper<String, String>)Either::left);
	}

	public static HttpDecoder<String> ofGet(String paramName, String defaultValue) {
		return ofGetEx(paramName, (HttpMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpDecoder<T> ofGet(String paramName, Function<String, T> fn, String message) {
		return ofGetEx(paramName, HttpMapper.of(fn, message));
	}

	public static <T> HttpDecoder<T> ofGet(String paramName, Function<String, T> fn, T defaultValue) {
		return ofGetEx(paramName, HttpMapper.of(fn), defaultValue);
	}

	public static <T> HttpDecoder<T> ofGetEx(@NotNull String paramName,
												 @NotNull HttpMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getQueryParameter, REQUIRED_GET_PARAM);
	}

	public static <T> HttpDecoder<T> ofGetEx(@NotNull String paramName,
												 @NotNull HttpMapper<String, T> fn,
												 @Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getQueryParameter, defaultValue);
	}

	public static HttpDecoder<String> ofPost(String paramName) {
		return ofPostEx(paramName, (HttpMapper<String, String>) Either::left);
	}

	public static HttpDecoder<String> ofPost(String paramName, String defaultValue) {
		return ofPostEx(paramName, (HttpMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpDecoder<T> ofPost(String paramName, Function<String, T> fn, String message) {
		return ofPostEx(paramName, HttpMapper.of(fn, message));
	}

	public static <T> HttpDecoder<T> ofPost(String paramName, Function<String, T> fn, T defaultValue) {
		return ofPostEx(paramName, HttpMapper.of(fn), defaultValue);
	}

	public static <T> HttpDecoder<T> ofPostEx(@NotNull String paramName,
												  @NotNull HttpMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getPostParameter, REQUIRED_POST_PARAM);
	}

	public static <T> HttpDecoder<T> ofPostEx(@NotNull String paramName,
												  @NotNull HttpMapper<String, T> fn,
												  @Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getPostParameter, defaultValue);
	}

	public static HttpDecoder<String> ofPath(String paramName) {
		return ofPathEx(paramName, (HttpMapper<String, String>) Either::left);
	}

	public static HttpDecoder<String> ofPath(String paramName, String defaultValue) {
		return ofPathEx(paramName, (HttpMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpDecoder<T> ofPath(String paramName, Function<String, T> fn, String message) {
		return ofPathEx(paramName, HttpMapper.of(fn, message));
	}

	public static <T> HttpDecoder<T> ofPath(String paramName, Function<String, T> fn, T defaultValue) {
		return ofPathEx(paramName, HttpMapper.of(fn), defaultValue);
	}

	public static <T> HttpDecoder<T> ofPathEx(@NotNull String paramName,
												  @NotNull HttpMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getPathParameter, REQUIRED_PATH_PARAM);
	}

	public static <T> HttpDecoder<T> ofPathEx(@NotNull String paramName,
												  @NotNull HttpMapper<String, T> fn,
												  @Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getPathParameter, defaultValue);
	}

	public static HttpDecoder<String> ofCookie(String paramName) {
		return ofCookieEx(paramName, (HttpMapper<String, String>) Either::left);
	}

	public static HttpDecoder<String> ofCookie(String paramName, String defaultValue) {
		return ofCookieEx(paramName, (HttpMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpDecoder<T> ofCookie(String paramName, Function<String, T> fn, String message) {
		return ofCookieEx(paramName, HttpMapper.of(fn, message));
	}

	public static <T> HttpDecoder<T> ofCookie(String paramName, Function<String, T> fn, T defaultValue) {
		return ofCookieEx(paramName, HttpMapper.of(fn), defaultValue);
	}

	public static <T> HttpDecoder<T> ofCookieEx(@NotNull String paramName,
													@NotNull HttpMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getCookie, REQUIRED_COOKIE);
	}

	public static <T> HttpDecoder<T> ofCookieEx(@NotNull String paramName,
													@NotNull HttpMapper<String, T> fn,
													@Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getCookie, defaultValue);
	}
}
