package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.promise.Async;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class LoggableServlet implements AsyncServlet {
	private static final Logger logger = LoggerFactory.getLogger(LoggableServlet.class);
	private static final BiFunction<HttpRequest, HttpResponse, String> DEFAULT_LOGGER_FUNCTION =
			(req, res) -> {
				ByteBuf body;
				int code = -1;
				String message = null;
				if (res != null) {
					code = res.getCode();
					body = res.isBodyLoaded() ? res.getBody() : null;
					message = (body != null && (code >= 400 && code < 500)) ? body.asString(UTF_8) : null;
				}
				return "HttpRequest[url: '" + (req != null ? req.getUrl() : "") + "'], " +
						"HttpResponse[code: " + (code != -1 ? code : "") + (message != null ? " message:'" + message + "'" : "") + "]";
			};

	private final AsyncServlet rootServlet;
	private final BiFunction<HttpRequest, HttpResponse, String> loggerFunction;

	private LoggableServlet(AsyncServlet rootServlet, BiFunction<HttpRequest, HttpResponse, String> loggerFunction) {
		this.rootServlet = rootServlet;
		this.loggerFunction = loggerFunction;
	}

	public static LoggableServlet create(AsyncServlet rootServlet) {
		return new LoggableServlet(rootServlet, DEFAULT_LOGGER_FUNCTION);
	}

	public static LoggableServlet create(AsyncServlet rootServlet, BiFunction<HttpRequest, @Nullable HttpResponse, String> loggerFunction) {
		return new LoggableServlet(rootServlet, loggerFunction);
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		Promise<HttpResponse> httpResponsePromise = rootServlet.serve(request).get();
		if (!httpResponsePromise.isComplete()) {
			logger.trace(loggerFunction.apply(request, null));
		}
		return httpResponsePromise
				.whenComplete((res, e) -> {
					if (e instanceof HttpException) {
						logger.info(loggerFunction.apply(request, ((HttpException) e).createResponse()));
					} else if (e != null) {
						logger.error("Exception", e);
					} else {
						logger.info(loggerFunction.apply(request, res));
					}
				});
	}
}
