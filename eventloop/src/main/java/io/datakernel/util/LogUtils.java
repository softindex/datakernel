package io.datakernel.util;

import io.datakernel.annotation.Nullable;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.datakernel.util.LogUtils.Level.INFO;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static java.util.stream.Collectors.joining;

public class LogUtils {
	public enum Level {
		OFF, TRACE, DEBUG, INFO, WARN, ERROR
	}

	private static final int LIST_LIMIT = 100;

	public static String thisMethod() {
		try {
			StackTraceElement stackTraceElement = Thread.currentThread().getStackTrace()[2];
			return stackTraceElement.getMethodName();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> BiConsumer<T, Throwable> toLogger(Logger logger,
														Level callLevel, Supplier<String> callMsg,
														Level resultLevel, Function<T, String> resultMsg,
														@Nullable Level errorLevel, Function<Throwable, String> errorMsg) {
		log(logger, callLevel, callMsg);
		return (result, throwable) -> {
			if (throwable == null) {
				log(logger, resultLevel, () -> resultMsg.apply(result));
			} else {
				if (errorLevel == null) {
					if (logger.isErrorEnabled()) {
						logger.error(errorMsg.apply(throwable), throwable);
					}
				} else {
					log(logger, errorLevel, () -> errorMsg.apply(throwable));
				}
			}
		};
	}

	public static <T> BiConsumer<T, Throwable> toLogger(Logger logger,
												Level callLevel, Supplier<String> callMsg,
												Level resultLevel, Function<T, String> resultMsg) {
		return toLogger(logger,
			callLevel, callMsg,
			resultLevel, resultMsg,
			null, throwable -> callMsg.get());
	}

	public static <T> BiConsumer<T, Throwable> toLogger(Logger logger,
														Level callLevel, Level resultLevel, @Nullable Level errorLevel,
														String methodName, Object... parameters) {
		return toLogger(logger,
			callLevel, () -> formatCall(methodName, parameters),
			resultLevel, result -> formatResult(methodName, result, parameters),
			errorLevel, errorLevel == null ?
				throwable -> formatCall(methodName, parameters) :
				throwable -> formatResult(methodName, throwable, parameters));
	}

	public static <T> BiConsumer<T, Throwable> toLogger(Logger logger,
												Level callLevel, Level resultLevel,
												String methodName, Object... parameters) {
		return toLogger(logger, callLevel, resultLevel, null, methodName, parameters);
	}

	public static <T> BiConsumer<T, Throwable> toLogger(Logger logger,
												Level level,
												String methodName, Object... parameters) {
		return toLogger(logger, level, level, methodName, parameters);
	}

	public static <T> BiConsumer<T, Throwable> toLogger(Logger logger, String methodName, Object... parameters) {
		return toLogger(logger, TRACE, INFO, methodName, parameters);
	}

	public static void log(Logger logger, Level level, Supplier<String> messageSupplier) {
		switch (level) {
			case TRACE:
				if (logger.isTraceEnabled()) {
					logger.trace(messageSupplier.get());
				}
				break;
			case DEBUG:
				if (logger.isDebugEnabled()) {
					logger.debug(messageSupplier.get());
				}
				break;
			case INFO:
				if (logger.isInfoEnabled()) {
					logger.info(messageSupplier.get());
				}
				break;
			case WARN:
				if (logger.isWarnEnabled()) {
					logger.warn(messageSupplier.get());
				}
				break;
			case ERROR:
				if (logger.isErrorEnabled()) {
					logger.error(messageSupplier.get());
				}
				break;
		}
	}

	private static String toString(Object object) {
		if (object == null) {
			return "null";
		}
		if (object instanceof Collection) {
			return CollectionUtils.toLimitedString((Collection<?>) object, LIST_LIMIT);
		}
		return object.toString();
	}

	public static String formatCall(String methodName, Object... parameters) {
		return methodName +
			(parameters.length != 0 ? " " + Arrays.stream(parameters)
				.map(LogUtils::toString)
				.collect(joining(", ")) : "") +
			" ...";
	}

	public static String formatResult(String methodName, Object result, Object... parameters) {
		return methodName +
			(parameters.length != 0 ? " " + Arrays.stream(parameters)
				.map(LogUtils::toString)
				.collect(joining(", ")) : "") +
			" -> " + toString(result);
	}
}
