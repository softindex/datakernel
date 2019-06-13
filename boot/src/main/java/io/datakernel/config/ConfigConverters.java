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

package io.datakernel.config;

import io.datakernel.async.EventloopTaskScheduler.Schedule;
import io.datakernel.async.RetryPolicy;
import io.datakernel.eventloop.FatalErrorHandler;
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.exception.ParseException;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.SimpleThreadFactory;
import io.datakernel.util.StringFormatUtils;
import io.datakernel.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.datakernel.eventloop.FatalErrorHandlers.*;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_KEYS_PER_SECOND;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_THROTTLING;
import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.util.Utils.*;
import static java.util.Collections.emptyList;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"unused", "WeakerAccess"})
public final class ConfigConverters {

	private ConfigConverters() {
	}

	public static ConfigConverter<LocalDate> ofLocalDate() {
		return new SimpleConfigConverter<LocalDate>() {
			@Override
			protected LocalDate fromString(String string) {
				return LocalDate.parse(string);
			}

			@Override
			protected String toString(LocalDate value) {
				return value.toString();
			}
		};
	}

	public static ConfigConverter<LocalTime> ofLocalTime() {
		return new SimpleConfigConverter<LocalTime>() {
			@Override
			protected LocalTime fromString(String string) {
				return LocalTime.parse(string);
			}

			@Override
			protected String toString(LocalTime value) {
				return value.toString();
			}
		};
	}

	public static ConfigConverter<LocalDateTime> ofLocalDateTime() {
		return new SimpleConfigConverter<LocalDateTime>() {
			@Override
			protected LocalDateTime fromString(String string) {
				return StringFormatUtils.parseLocalDateTime(string);
			}

			@Override
			protected String toString(LocalDateTime value) {
				return StringFormatUtils.formatLocalDateTime(value);
			}
		};
	}

	public static ConfigConverter<Period> ofPeriod() {
		return new SimpleConfigConverter<Period>() {
			@Override
			protected Period fromString(String string) {
				return StringFormatUtils.parsePeriod(string);
			}

			@Override
			protected String toString(Period value) {
				return StringFormatUtils.formatPeriod(value);
			}
		};
	}

	/**
	 * @return config converter with days in period
	 */
	public static ConfigConverter<Integer> ofPeriodAsDays() {
		return ofPeriod().transform(Period::getDays, Period::ofDays);
	}

	public static ConfigConverter<Duration> ofDuration() {
		return new SimpleConfigConverter<Duration>() {
			@Override
			protected Duration fromString(String string) {
				return StringFormatUtils.parseDuration(string);
			}

			@Override
			protected String toString(Duration value) {
				return StringFormatUtils.formatDuration(value);
			}
		};
	}

	/**
	 * @return config converter with millis in duration
	 */
	public static ConfigConverter<Long> ofDurationAsMillis() {
		return ofDuration().transform(Duration::toMillis, Duration::ofMillis);
	}

	public static ConfigConverter<Instant> ofInstant() {
		return new SimpleConfigConverter<Instant>() {
			@Override
			protected Instant fromString(String string) {
				return StringFormatUtils.parseInstant(string);
			}

			@Override
			protected String toString(Instant value) {
				return StringFormatUtils.formatInstant(value);
			}
		};
	}

	/**
	 * @return config converter with epoch millis in instant
	 */
	public static ConfigConverter<Long> ofInstantAsEpochMillis() {
		return ofInstant().transform(Instant::toEpochMilli, Instant::ofEpochMilli);
	}

	public static ConfigConverter<String> ofString() {
		return new ConfigConverter<String>() {
			@Override
			public String get(Config config, String defaultValue) {
				return config.getValue(defaultValue);
			}

			@NotNull
			@Override
			public String get(Config config) {
				return get(config, "");
			}
		};
	}

	public static ConfigConverter<String> ofNullableString() {
		return new SimpleConfigConverter<String>() {
			@Override
			protected String fromString(String string) {
				return string;
			}

			@Override
			protected String toString(String value) {
				return value;
			}
		};
	}

	public static ConfigConverter<Byte> ofByte() {
		return new SimpleConfigConverter<Byte>() {
			@Override
			protected Byte fromString(String string) {
				return Byte.valueOf(string);
			}

			@Override
			protected String toString(Byte value) {
				return Byte.toString(value);
			}
		};
	}

	public static ConfigConverter<Integer> ofInteger() {
		return new SimpleConfigConverter<Integer>() {
			@Override
			protected Integer fromString(String string) {
				return Integer.valueOf(string);
			}

			@Override
			protected String toString(Integer value) {
				return Integer.toString(value);
			}
		};
	}

	public static ConfigConverter<Long> ofLong() {
		return new SimpleConfigConverter<Long>() {
			@Override
			public Long fromString(String string) {
				return Long.parseLong(string);
			}

			@Override
			public String toString(Long value) {
				return Long.toString(value);
			}
		};
	}

	public static ConfigConverter<Float> ofFloat() {
		return new SimpleConfigConverter<Float>() {
			@Override
			public Float fromString(String string) {
				return Float.parseFloat(string);
			}

			@Override
			public String toString(Float value) {
				return Float.toString(value);
			}
		};
	}

	public static ConfigConverter<Double> ofDouble() {
		return new SimpleConfigConverter<Double>() {
			@Override
			public Double fromString(String string) {
				return Double.parseDouble(string);
			}

			@Override
			public String toString(Double value) {
				return Double.toString(value);
			}
		};
	}

	public static ConfigConverter<Boolean> ofBoolean() {
		return new SimpleConfigConverter<Boolean>() {
			@Override
			public Boolean fromString(String string) {
				return Boolean.parseBoolean(string);
			}

			@Override
			public String toString(Boolean value) {
				return Boolean.toString(value);
			}
		};
	}

	public static <E extends Enum<E>> SimpleConfigConverter<E> ofEnum(Class<E> enumClass) {
		Class<E> enumClass1 = enumClass;
		return new SimpleConfigConverter<E>() {
			private final Class<E> enumClass = enumClass1;

			@Override
			public E fromString(String string) {
				return Enum.valueOf(enumClass, string);
			}

			@Override
			public String toString(E value) {
				return value.name();
			}
		};
	}

	public static ConfigConverter<Class<?>> ofClass() {
		return new SimpleConfigConverter<Class<?>>() {
			@Override
			public Class<?> fromString(String string) {
				try {
					return Class.forName(string);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@Override
			public String toString(Class<?> value) {
				return value.getName();
			}
		};
	}

	public static ConfigConverter<InetAddress> ofInetAddress() {
		return new SimpleConfigConverter<InetAddress>() {
			@Override
			public InetAddress fromString(String address) {
				try {
					return InetAddress.getByName(address);
				} catch (UnknownHostException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@Override
			public String toString(InetAddress value) {
				return Arrays.toString(value.getAddress());
			}
		};
	}

	public static ConfigConverter<InetSocketAddress> ofInetSocketAddress() {
		return new SimpleConfigConverter<InetSocketAddress>() {
			@Override
			public InetSocketAddress fromString(String addressPort) {
				try {
					return parseInetSocketAddress(addressPort);
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@Override
			public String toString(InetSocketAddress value) {
				return value.getAddress().getHostAddress() + ":" + value.getPort();
			}
		};
	}

	public static ConfigConverter<Path> ofPath() {
		return new SimpleConfigConverter<Path>() {
			@Override
			protected Path fromString(String string) {
				return Paths.get(string);
			}

			@Override
			protected String toString(Path value) {
				return value.toAbsolutePath().normalize().toString();
			}
		};
	}

	public static ConfigConverter<MemSize> ofMemSize() {
		return new SimpleConfigConverter<MemSize>() {
			@Override
			public MemSize fromString(String string) {
				return MemSize.valueOf(string);
			}

			@Override
			public String toString(MemSize value) {
				return value.format();
			}
		};
	}

	/**
	 * @return config converter with bytes in memsize
	 */
	public static ConfigConverter<Long> ofMemSizeAsLong() {
		return ofMemSize().transform(MemSize::toLong, MemSize::of);
	}

	/**
	 * @return config converter with bytes in memsize
	 */
	public static ConfigConverter<Integer> ofMemSizeAsInt() {
		return ofMemSize().transform(MemSize::toInt, (Function<Integer, MemSize>) MemSize::of);
	}

	public static ConfigConverter<InetAddressRange> ofInetAddressRange() {
		return new SimpleConfigConverter<InetAddressRange>() {
			@Override
			public InetAddressRange fromString(String string) {
				try {
					return InetAddressRange.parse(string);
				} catch (ParseException e) {
					throw new IllegalArgumentException("Can't parse inetAddressRange config", e);
				}
			}

			@Override
			public String toString(InetAddressRange value) {
				return value.toString();
			}
		};
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverter<T> elementConverter, CharSequence separators) {
		return new SimpleConfigConverter<List<T>>() {
			private final Pattern pattern = compile(separators.chars()
					.mapToObj(c -> "\\" + (char) c)
					.collect(joining("", "[", "]")));

			@Override
			public List<T> fromString(String string) {
				return pattern.splitAsStream(string)
						.map(String::trim)
						.filter(s -> !s.isEmpty())
						.map(s -> elementConverter.get(Config.ofValue(s)))
						.collect(toList());
			}

			@Override
			public String toString(List<T> value) {
				return value.stream()
						.map(v -> {
							Config config = Config.ofValue(elementConverter, v);
							if (config.hasChildren()) {
								throw new AssertionError("Unexpected child entries: " + config.toMap());
							}
							return config.getValue();
						})
						.collect(joining(String.valueOf(separators.charAt(0))));
			}
		};
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverter<T> elementConverter) {
		return ofList(elementConverter, ",;");
	}

	// compound
	public static ConfigConverter<ServerSocketSettings> ofServerSocketSettings() {
		return new ComplexConfigConverter<ServerSocketSettings>(ServerSocketSettings.create(DEFAULT_BACKLOG)) {
			@Override
			protected ServerSocketSettings provide(Config config, ServerSocketSettings defaultValue) {
				return Function.<ServerSocketSettings>identity()
						.andThen(apply(
								ServerSocketSettings::withBacklog,
								config.get(ofInteger(), "backlog", defaultValue.getBacklog())))
						.andThen(applyIfNotNull(
								ServerSocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? defaultValue.getReceiveBufferSize() : null)))
						.andThen(applyIfNotNull(
								ServerSocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.apply(ServerSocketSettings.create(DEFAULT_BACKLOG));
			}
		};
	}

	public static ConfigConverter<SocketSettings> ofSocketSettings() {
		return new ComplexConfigConverter<SocketSettings>(SocketSettings.create()) {
			@Override
			protected SocketSettings provide(Config config, SocketSettings defaultValue) {
				return Function.<SocketSettings>identity()
						.andThen(applyIfNotNull(
								SocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? defaultValue.getReceiveBufferSize() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withSendBufferSize,
								config.get(ofMemSize(), "sendBufferSize",
										defaultValue.hasSendBufferSize() ? defaultValue.getSendBufferSize() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withKeepAlive,
								config.get(ofBoolean(), "keepAlive",
										defaultValue.hasKeepAlive() ? defaultValue.getKeepAlive() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withTcpNoDelay,
								config.get(ofBoolean(), "tcpNoDelay",
										defaultValue.hasTcpNoDelay() ? defaultValue.getTcpNoDelay() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withImplReadTimeout,
								config.get(ofDuration(), "implReadTimeout",
										defaultValue.hasImplReadTimeout() ? defaultValue.getImplReadTimeout() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withImplWriteTimeout,
								config.get(ofDuration(), "implWriteTimeout",
										defaultValue.hasImplWriteTimeout() ? defaultValue.getImplWriteTimeout() : null)))
						.andThen(applyIfNotNull(
								SocketSettings::withImplReadBufferSize,
								config.get(ofMemSize(), "implReadBufferSize",
										defaultValue.hasReadBufferSize() ? defaultValue.getImplReadBufferSize() : null)))
						.apply(SocketSettings.create());
			}
		};
	}

	public static ConfigConverter<DatagramSocketSettings> ofDatagramSocketSettings() {
		return new ComplexConfigConverter<DatagramSocketSettings>(DatagramSocketSettings.create()) {
			@Override
			protected DatagramSocketSettings provide(Config config, DatagramSocketSettings defaultValue) {
				return Function.<DatagramSocketSettings>identity()
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? defaultValue.getReceiveBufferSize() : null)))
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withSendBufferSize,
								config.get(ofMemSize(), "sendBufferSize",
										defaultValue.hasSendBufferSize() ? defaultValue.getSendBufferSize() : null)))
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.andThen(applyIfNotNull(
								DatagramSocketSettings::withBroadcast,
								config.get(ofBoolean(), "broadcast",
										defaultValue.hasBroadcast() ? defaultValue.getBroadcast() : null)))
						.apply(DatagramSocketSettings.create());
			}
		};
	}

	public static final ConfigConverter<List<Class<?>>> OF_CLASSES = ofList(ofClass());

	public static ConfigConverter<FatalErrorHandler> ofFatalErrorHandler() {
		return new ConfigConverter<FatalErrorHandler>() {
			@NotNull
			@Override
			public FatalErrorHandler get(Config config) {
				switch (config.getValue()) {
					case "rethrowOnAnyError":
						return rethrowOnAnyError();
					case "ignoreAllErrors":
						return ignoreAllErrors();
					case "exitOnAnyError":
						return exitOnAnyError();
					case "exitOnJvmError":
						return exitOnJvmError();
					case "rethrowOnMatchedError":
						return rethrowOnMatchedError(
								config.get(OF_CLASSES, "whitelist", emptyList()),
								config.get(OF_CLASSES, "blacklist", emptyList()));
					case "exitOnMatchedError":
						return exitOnMatchedError(
								config.get(OF_CLASSES, "whitelist", emptyList()),
								config.get(OF_CLASSES, "blacklist", emptyList()));
					default:
						throw new IllegalArgumentException("No fatal error handler named " + config.getValue() + " exists!");
				}
			}

			@Override
			public FatalErrorHandler get(Config config, FatalErrorHandler defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}

	public static ConfigConverter<Schedule> ofEventloopTaskSchedule() {
		return new ConfigConverter<Schedule>() {
			@NotNull
			@Override
			public Schedule get(Config config) {
				switch (config.get("type")) {
					case "immediate":
						return Schedule.immediate();
					case "delay":
						return Schedule.ofDelay(config.get(ofDuration(), "value"));
					case "interval":
						return Schedule.ofInterval(config.get(ofDuration(), "value"));
					case "period":
						return Schedule.ofPeriod(config.get(ofDuration(), "value"));
					default:
						throw new IllegalArgumentException("No eventloop task schedule type named " + config.getValue() + " exists!");
				}
			}

			@Override
			public Schedule get(Config config, Schedule defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}

	public static ConfigConverter<RetryPolicy> ofRetryPolicy() {
		return new ConfigConverter<RetryPolicy>() {
			@NotNull
			@Override
			public RetryPolicy get(Config config) {
				if (!config.hasValue() || config.getValue().equals("no")) {
					return RetryPolicy.noRetry();
				}
				RetryPolicy retryPolicy;
				switch (config.getValue()) {
					case "immediate":
						retryPolicy = RetryPolicy.immediateRetry();
						break;
					case "fixedDelay":
						retryPolicy = RetryPolicy.fixedDelay(config.get(ofDuration(), "delay").toMillis());
						break;
					case "exponentialBackoff":
						retryPolicy = RetryPolicy.exponentialBackoff(config.get(ofDuration(), "initialDelay").toMillis(),
								config.get(ofDuration(), "maxDelay").toMillis(), config.get(ofDouble(), "exponent", 2.0));
						break;
					default:
						throw new IllegalArgumentException("No retry policy named " + config.getValue() + " exists!");
				}
				int maxRetryCount = config.get(ofInteger(), "maxRetryCount", Integer.MAX_VALUE);
				if (maxRetryCount != Integer.MAX_VALUE) {
					retryPolicy = retryPolicy.withMaxTotalRetryCount(maxRetryCount);
				}
				Duration max = Duration.ofSeconds(Long.MAX_VALUE);
				Duration maxRetryTimeout = config.get(ofDuration(), "maxRetryTimeout", max);
				if (!maxRetryTimeout.equals(max)) {
					retryPolicy = retryPolicy.withMaxTotalRetryTimeout(maxRetryTimeout);
				}
				return retryPolicy;
			}

			@Override
			public RetryPolicy get(Config config, RetryPolicy defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}
		};
	}

	public static ConfigConverter<ThrottlingController> ofThrottlingController() {
		return new ComplexConfigConverter<ThrottlingController>(ThrottlingController.create()) {
			@Override
			protected ThrottlingController provide(Config config, ThrottlingController defaultValue) {
				return ThrottlingController.create()
						.withTargetTime(config.get(ofDuration(), "targetTime", defaultValue.getTargetTime()))
						.withGcTime(config.get(ofDuration(), "gcTime", defaultValue.getGcTime()))
						.withSmoothingWindow(config.get(ofDuration(), "smoothingWindow", defaultValue.getSmoothingWindow()))
						.withThrottlingDecrease(config.get(ofDouble(), "throttlingDecrease", defaultValue.getThrottlingDecrease()))
						.withInitialKeysPerSecond(config.get(ofDouble(), "initialKeysPerSecond", INITIAL_KEYS_PER_SECOND))
						.withInitialThrottling(config.get(ofDouble(), "initialThrottling", INITIAL_THROTTLING));
			}
		};
	}

	public static ConfigConverter<SimpleThreadFactory> ofThreadFactory() {
		return new ComplexConfigConverter<SimpleThreadFactory>(SimpleThreadFactory.create()) {
			@Override
			protected SimpleThreadFactory provide(Config config, SimpleThreadFactory defaultValue) {
				SimpleThreadFactory result = SimpleThreadFactory.create();
				String threadGroupName = config.get(ofNullableString(), "threadGroup", Utils.transform(defaultValue.getThreadGroup(), ThreadGroup::getName));
				if (threadGroupName != null) {
					result.withThreadGroup(new ThreadGroup(threadGroupName));
				}
				return result
						.withName(config.get(ofNullableString(), "name", defaultValue.getName()))
						.withPriority(config.get(ofInteger(), "priority", defaultValue.getPriority()))
						.withDaemon(config.get(ofBoolean(), "daemon", defaultValue.isDaemon()));
			}
		};
	}

	public static ExecutorService getExecutor(Config config) {
		int corePoolSize = config.get(ofInteger().withConstraint(x -> x >= 0), "corePoolSize", 0);
		int maxPoolSize = config.get(ofInteger().withConstraint(x -> x == 0 || x >= corePoolSize), "maxPoolSize", 0);
		int keepAlive = config.get(ofInteger().withConstraint(x -> x >= 0), "keepAliveSeconds", 60);
		return new ThreadPoolExecutor(
				corePoolSize,
				maxPoolSize == 0 ? Integer.MAX_VALUE : maxPoolSize,
				keepAlive,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());
	}

	public static ConfigConverter<ExecutorService> ofExecutor() {
		return new ConfigConverter<ExecutorService>() {
			@NotNull
			@Override
			public ExecutorService get(Config config) {
				return getExecutor(config);
			}

			@Override
			public ExecutorService get(Config config, ExecutorService defaultValue) {
				throw new IllegalArgumentException("Should not use executor config converter with default value");
			}
		};
	}
}
