package io.datakernel.http.parser;

import io.datakernel.exception.ParseException;
import io.datakernel.http.HttpRequest;
import io.datakernel.util.ParserFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

public final class HttpParamParsers {
	public static HttpParamParser<String> ofGet(String paramName) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) throws ParseException {
				String queryParameter = request.getQueryParameter(paramName);
				if (queryParameter == null) {
					throw new ParseException(String.format("Parse exception on field %s", paramName));
				}
				return queryParameter;
			}
		}.withId(paramName);
	}

	public static HttpParamParser<String> ofGet(String paramName, String defaultValue) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) {
				String queryParameter = request.getQueryParameter(paramName);
				return queryParameter == null ? defaultValue : queryParameter;
			}
		}.withId(paramName);
	}

	public static <T> HttpParamParser<T> ofGet(@NotNull String paramName,
											   @NotNull Function<String, T> fn,
											   @Nullable T defaultValue) {
		ParserFunction<String, T> parserFn = ParserFunction.of(fn);
		return new HttpParamParserImpl<T>() {
			@Override
			public T parse(@NotNull HttpRequest request) {
				String queryParameter = request.getQueryParameter(paramName);
				return parserFn.parseOrDefault(queryParameter, defaultValue);
			}
		}.withId(paramName);
	}

	public static HttpParamParser<String> ofPost(String paramName) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) throws ParseException {
				String postParameter = request.getPostParameter(paramName);
				if (postParameter == null) {
					throw new ParseException(String.format("Parse exception on field %s", paramName));
				}
				return postParameter;
			}
		}.withId(paramName);
	}

	public static HttpParamParser<String> ofPost(String paramName, String defaultValue) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) {
				String postParameter = request.getPostParameter(paramName);
				return postParameter == null ? defaultValue : postParameter;
			}
		}.withId(paramName);
	}

	public static <T> HttpParamParser<T> ofPost(String paramName, Function<String, T> fn, T defaultValue) {
		ParserFunction<String, T> parserFn = ParserFunction.of(fn);
		return new HttpParamParserImpl<T>() {
			@Override
			public T parse(@NotNull HttpRequest request) {
				String postParameter = request.getPostParameter(paramName);
				return parserFn.parseOrDefault(postParameter, defaultValue);
			}
		}.withId(paramName);
	}

	public static HttpParamParser<String> ofPath(String paramName) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) throws ParseException {
				String pathParameter = request.getPathParameter(paramName);
				if (pathParameter == null) {
					throw new ParseException(String.format("Parse exception on field %s", paramName));
				}
				return pathParameter;
			}
		}.withId(paramName);
	}

	public static HttpParamParser<String> ofPath(String paramName, String defaultValue) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) {
				String pathParameter = request.getPathParameter(paramName);
				return pathParameter == null ? defaultValue : pathParameter;
			}
		}.withId(paramName);
	}

	public static <T> HttpParamParser<T> ofPath(String paramName, Function<String, T> fn, T defaultValue) {
		ParserFunction<String, T> parserFn = ParserFunction.of(fn);
		return new HttpParamParserImpl<T>() {
			@Override
			public T parse(@NotNull HttpRequest request) {
				String pathParameter = request.getPathParameter(paramName);
				return parserFn.parseOrDefault(pathParameter, defaultValue);
			}
		}.withId(paramName);
	}

	public static HttpParamParser<String> ofCookie(String paramName) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) throws ParseException {
				String cookie = request.getCookie(paramName);
				if (cookie == null) {
					throw new ParseException(String.format("Parse exception on field %s", paramName));
				}
				return cookie;
			}
		}.withId(paramName);
	}

	public static HttpParamParser<String> ofCookie(String paramName, String defaultValue) {
		return new HttpParamParserImpl<String>() {
			@Override
			public String parse(@NotNull HttpRequest request) {
				String cookie = request.getCookie(paramName);
				return cookie == null ? defaultValue : cookie;
			}
		}.withId(paramName);
	}

	public static <T> HttpParamParser<T> ofCookie(String paramName, Function<String, T> fn, T defaultValue) {
		ParserFunction<String, T> parserFn = ParserFunction.of(fn);
		return new HttpParamParserImpl<T>() {
			@Override
			public T parse(@NotNull HttpRequest request) {
				String cookie = request.getCookie(paramName);
				return parserFn.parseOrDefault(cookie, defaultValue);
			}
		}.withId(paramName);
	}
}
