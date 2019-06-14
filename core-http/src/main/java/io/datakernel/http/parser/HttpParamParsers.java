package io.datakernel.http.parser;

import io.datakernel.functional.Either;
import io.datakernel.http.HttpRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings("RedundantCast")
public final class HttpParamParsers {
	public static final String REQUIRED_GET_PARAM = "Required GET param: %1";
	public static final String REQUIRED_POST_PARAM = "Required POST param: %1";
	public static final String REQUIRED_PATH_PARAM = "Required path param";
	public static final String REQUIRED_COOKIE = "Required cookie: %1";

	private static <T> HttpParamParser<T> ofParamEx(String paramName,
													@NotNull HttpParamMapper<String, T> fn,
													@NotNull BiFunction<HttpRequest, String, String> paramSupplier,
			String message) {
		return new AbstractHttpParamParser<T>(paramName) {
			@Override
			public Either<T, HttpParamParseErrorsTree> parse(@NotNull HttpRequest request) {
				String str = paramSupplier.apply(request, paramName);
				return str != null ?
						fn.map(str)
								.mapRight(HttpParamParseErrorsTree::of) :
						Either.right(HttpParamParseErrorsTree.of(message, paramName));
			}
		};
	}

	private static <T> HttpParamParser<T> ofParamEx(String paramName,
													@NotNull HttpParamMapper<String, T> fn,
													@NotNull BiFunction<HttpRequest, String, String> paramSupplier,
													@Nullable T defaultValue) {
		return new AbstractHttpParamParser<T>(paramName) {
			@Override
			public Either<T, HttpParamParseErrorsTree> parse(@NotNull HttpRequest request) {
				String str = paramSupplier.apply(request, paramName);
				return str != null ?
						fn.map(str)
								.mapRight(HttpParamParseErrorsTree::of) :
						Either.left(defaultValue);
			}
		};
	}

	public static HttpParamParser<String> ofGet(String paramName) {
		return ofGetEx(paramName, (HttpParamMapper<String, String>)Either::left);
	}

	public static HttpParamParser<String> ofGet(String paramName, String defaultValue) {
		return ofGetEx(paramName, (HttpParamMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpParamParser<T> ofGet(String paramName, Function<String, T> fn, String message) {
		return ofGetEx(paramName, HttpParamMapper.of(fn, message));
	}

	public static <T> HttpParamParser<T> ofGet(String paramName, Function<String, T> fn, T defaultValue) {
		return ofGetEx(paramName, HttpParamMapper.of(fn), defaultValue);
	}

	public static <T> HttpParamParser<T> ofGetEx(@NotNull String paramName,
												 @NotNull HttpParamMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getQueryParameter, REQUIRED_GET_PARAM);
	}

	public static <T> HttpParamParser<T> ofGetEx(@NotNull String paramName,
												 @NotNull HttpParamMapper<String, T> fn,
												 @Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getQueryParameter, defaultValue);
	}

	public static HttpParamParser<String> ofPost(String paramName) {
		return ofPostEx(paramName, (HttpParamMapper<String, String>) Either::left);
	}

	public static HttpParamParser<String> ofPost(String paramName, String defaultValue) {
		return ofPostEx(paramName, (HttpParamMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpParamParser<T> ofPost(String paramName, Function<String, T> fn, String message) {
		return ofPostEx(paramName, HttpParamMapper.of(fn, message));
	}

	public static <T> HttpParamParser<T> ofPost(String paramName, Function<String, T> fn, T defaultValue) {
		return ofPostEx(paramName, HttpParamMapper.of(fn), defaultValue);
	}

	public static <T> HttpParamParser<T> ofPostEx(@NotNull String paramName,
												  @NotNull HttpParamMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getPostParameter, REQUIRED_POST_PARAM);
	}

	public static <T> HttpParamParser<T> ofPostEx(@NotNull String paramName,
												  @NotNull HttpParamMapper<String, T> fn,
												  @Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getPostParameter, defaultValue);
	}

	public static HttpParamParser<String> ofPath(String paramName) {
		return ofPathEx(paramName, (HttpParamMapper<String, String>) Either::left);
	}

	public static HttpParamParser<String> ofPath(String paramName, String defaultValue) {
		return ofPathEx(paramName, (HttpParamMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpParamParser<T> ofPath(String paramName, Function<String, T> fn, String message) {
		return ofPathEx(paramName, HttpParamMapper.of(fn, message));
	}

	public static <T> HttpParamParser<T> ofPath(String paramName, Function<String, T> fn, T defaultValue) {
		return ofPathEx(paramName, HttpParamMapper.of(fn), defaultValue);
	}

	public static <T> HttpParamParser<T> ofPathEx(@NotNull String paramName,
												  @NotNull HttpParamMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getPathParameter, REQUIRED_PATH_PARAM);
	}

	public static <T> HttpParamParser<T> ofPathEx(@NotNull String paramName,
												  @NotNull HttpParamMapper<String, T> fn,
												  @Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getPathParameter, defaultValue);
	}

	public static HttpParamParser<String> ofCookie(String paramName) {
		return ofCookieEx(paramName, (HttpParamMapper<String, String>) Either::left);
	}

	public static HttpParamParser<String> ofCookie(String paramName, String defaultValue) {
		return ofCookieEx(paramName, (HttpParamMapper<String, String>) Either::left, defaultValue);
	}

	public static <T> HttpParamParser<T> ofCookie(String paramName, Function<String, T> fn, String message) {
		return ofCookieEx(paramName, HttpParamMapper.of(fn, message));
	}

	public static <T> HttpParamParser<T> ofCookie(String paramName, Function<String, T> fn, T defaultValue) {
		return ofCookieEx(paramName, HttpParamMapper.of(fn), defaultValue);
	}

	public static <T> HttpParamParser<T> ofCookieEx(@NotNull String paramName,
													@NotNull HttpParamMapper<String, T> fn) {
		return ofParamEx(paramName, fn, HttpRequest::getCookie, REQUIRED_COOKIE);
	}

	public static <T> HttpParamParser<T> ofCookieEx(@NotNull String paramName,
													@NotNull HttpParamMapper<String, T> fn,
													@Nullable T defaultValue) {
		return ofParamEx(paramName, fn, HttpRequest::getCookie, defaultValue);
	}
}
