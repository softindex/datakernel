package io.datakernel.vlog.util;

import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.decoder.DecodeErrors;
import io.datakernel.promise.Promise;
import io.global.mustache.MustacheTemplater;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.http.HttpResponse.redirect302;
import static io.global.Utils.isGzipAccepted;
import static io.global.Utils.redirectToReferer;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpUtil {
	public static final String WHITESPACE = "^(?:\\p{Z}|\\p{C})*$";

	public static AsyncServletDecorator renderErrors(MustacheTemplater templater) {
		return servlet ->
				request ->
						servlet.serve(request)
								.get()
								.thenEx((response, e) -> {
									if (e != null) {
										int code = e instanceof HttpException ? ((HttpException) e).getCode() : 500;
										return templater.render(code, "error",
												map("code", code, "message", e.getMessage()),
												isGzipAccepted(request));
									}
									int code = response.getCode();
									if (code < 400) {
										return Promise.of(response);
									}
									String message = response.isBodyLoaded() ? response.getBody().getString(UTF_8) : "";
									return templater.render(code, "error",
											map("code", code, "message", message),
											isGzipAccepted(request));
								});
	}

	public static HttpException decodeErrorsToHttpException(DecodeErrors errors) {
		StringBuilder builder = new StringBuilder();
		errors.toMultimap().forEach((error, causes) -> {
			builder.append(error)
					.append(" -> ")
					.append(causes);
		});
		return HttpException.ofCode(403, builder.toString());
	}

	public static HttpResponse redirectToLogin(HttpRequest request) {
		return redirect302("/login?origin=" + request.getPath());
	}

	public static HttpResponse postOpRedirect(HttpRequest request) {
		String tid = request.getPathParameter("videoViewID");
		return redirectToReferer(request, "/" + tid);
	}

	public static boolean validate(String param, int maxLength) {
		return validate(param, maxLength, false);
	}

	public static boolean validate(@Nullable String param, int maxLength, boolean required) {
		if (param == null && required || (param != null && param.matches(WHITESPACE) && required)) {
			return false;
		}
		return param == null || param.length() <= maxLength;
	}
}
