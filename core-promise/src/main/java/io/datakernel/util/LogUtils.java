/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.util;

import io.datakernel.async.Callback;
import io.datakernel.exception.StacklessException;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static java.util.stream.Collectors.joining;

public class LogUtils {
	private static final int LIST_LIMIT = 100;

	public enum Level {
		OFF(null) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return false;
			}
		},

		FINEST((logger, msg) -> logger.log(java.util.logging.Level.FINEST, msg)) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isLoggable(java.util.logging.Level.FINEST);
			}
		},

		FINER((logger, msg) -> logger.log(java.util.logging.Level.FINER, msg)) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isLoggable(java.util.logging.Level.FINER);
			}
		},

		FINE((logger, msg) -> logger.log(java.util.logging.Level.FINE, msg)) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isLoggable(java.util.logging.Level.FINE);
			}
		},

		INFO((logger, msg) -> logger.log(java.util.logging.Level.INFO, msg)) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isLoggable(java.util.logging.Level.INFO);
			}
		},

		WARNING((logger, msg) -> logger.log(java.util.logging.Level.WARNING, msg)) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isLoggable(java.util.logging.Level.WARNING);
			}
		},

		SEVERE((logger, msg) -> logger.log(java.util.logging.Level.SEVERE, msg)) {
			@Override
			protected boolean isEnabled(Logger logger) {
				return logger.isLoggable(java.util.logging.Level.SEVERE);
			}
		};

		private final BiConsumer<Logger, String> logConsumer;

		Level(BiConsumer<Logger, String> logConsumer) {
			this.logConsumer = logConsumer;
		}

		protected abstract boolean isEnabled(Logger logger);

		public final void log(Logger logger, Supplier<String> messageSupplier) {
			if (isEnabled(logger)) {
				logConsumer.accept(logger, messageSupplier.get());
			}
		}

		public final void log(Logger logger, String message) {
			if (isEnabled(logger)) {
				logConsumer.accept(logger, message);
			}
		}
	}

	public static String thisMethod() {
		try {
			StackTraceElement stackTraceElement = Thread.currentThread().getStackTrace()[2];
			return stackTraceElement.getMethodName();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> Callback<T> toLogger(Logger logger,
										   Level callLevel, Supplier<String> callMsg,
										   Level resultLevel, Function<T, String> resultMsg,
										   @Nullable Level errorLevel, Function<Throwable, String> errorMsg) {
		if (!logger.isLoggable(java.util.logging.Level.SEVERE)) return ($, e) -> {};
		callLevel.log(logger, callMsg);
		return (result, e) -> {
			if (e == null) {
				resultLevel.log(logger, () -> resultMsg.apply(result));
			} else if (!(e instanceof StacklessException) || !((StacklessException) e).isConstant()) {
				if (errorLevel == null) {
					logger.log(java.util.logging.Level.SEVERE, e, () -> errorMsg.apply(e));
				} else {
					errorLevel.log(logger, () -> errorMsg.apply(e));
				}
			} else {
				resultLevel.log(logger, () -> errorMsg.apply(e));
			}
		};
	}

	public static <T> Callback<T> toLogger(Logger logger,
												 Level callLevel, Supplier<String> callMsg,
												 Level resultLevel, Function<T, String> resultMsg) {
		return toLogger(logger,
				callLevel, callMsg,
				resultLevel, resultMsg,
				null, e -> callMsg.get());
	}

	public static <T> Callback<T> toLogger(Logger logger,
												 Level callLevel, Level resultLevel, Level errorLevel,
												 String methodName, Object... parameters) {
		return toLogger(logger,
				callLevel, () -> formatCall(methodName, parameters),
				resultLevel, result -> formatResult(methodName, result, parameters),
				errorLevel, errorLevel == null ?
						e -> formatCall(methodName, parameters) :
						e -> formatResult(methodName, e, parameters));
	}

	public static <T> Callback<T> toLogger(Logger logger,
												 Level callLevel, Level resultLevel,
												 String methodName, Object... parameters) {
		return toLogger(logger, callLevel, resultLevel, null, methodName, parameters);
	}

	public static <T> Callback<T> toLogger(Logger logger,
												 Level level,
												 String methodName, Object... parameters) {
		return toLogger(logger, level, level, methodName, parameters);
	}

	public static <T> Callback<T> toLogger(Logger logger, String methodName, Object... parameters) {
		return toLogger(logger, Level.FINEST, Level.INFO, methodName, parameters);
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
