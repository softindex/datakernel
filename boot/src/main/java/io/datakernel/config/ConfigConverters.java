/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import com.zaxxer.hikari.HikariConfig;
import io.datakernel.eventloop.FatalErrorHandler;
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.exception.ParseException;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.SimpleThreadFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.datakernel.config.Config.ifNotDefault;
import static io.datakernel.config.Config.ifNotNull;
import static io.datakernel.eventloop.FatalErrorHandlers.*;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_KEYS_PER_SECOND;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_THROTTLING;
import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Utils.*;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"unused", "WeakerAccess"})
public final class ConfigConverters {

	private ConfigConverters() {
	}

	public static ConfigConverter<String> ofString() {
		return new ConfigConverter<String>() {
			@Override
			public String get(Config config, String defaultValue) {
				return config.getValue(defaultValue);
			}

			@Override
			public String get(Config config) {
				return get(config, "");
			}
		};
	}

	public static ConfigConverter<String> ofNullableString() {
		return new SimpleConfigConverter<String>() {
			@Override
			protected String fromString(String value) {
				return value;
			}

			@Override
			protected String toString(String defaultValue) {
				return defaultValue;
			}
		};
	}

	public static ConfigConverter<Byte> ofByte() {
		return new SimpleConfigConverter<Byte>() {
			@Override
			protected Byte fromString(String value) {
				return Byte.valueOf(value);
			}

			@Override
			protected String toString(Byte defaultValue) {
				return Byte.toString(defaultValue);
			}
		};
	}

	public static ConfigConverter<Integer> ofInteger() {
		return new SimpleConfigConverter<Integer>() {
			@Override
			protected Integer fromString(String value) {
				return Integer.valueOf(value);
			}

			@Override
			protected String toString(Integer defaultValue) {
				return Integer.toString(defaultValue);
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
			public String toString(Long defaultValue) {
				return Long.toString(defaultValue);
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
			public String toString(Float defaultValue) {
				return Float.toString(defaultValue);
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
			public String toString(Double defaultValue) {
				return Double.toString(defaultValue);
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
			public String toString(Boolean defaultValue) {
				return Boolean.toString(defaultValue);
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
			public String toString(E defaultValue) {
				return defaultValue.name();
			}
		};
	}

	public static ConfigConverter<Class> ofClass() {
		return new SimpleConfigConverter<Class>() {
			@Override
			public Class fromString(String string) {
				try {
					return Class.forName(string);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@Override
			public String toString(Class defaultValue) {
				return defaultValue.getName();
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
			public String toString(InetAddress item) {
				return Arrays.toString(item.getAddress());
			}
		};
	}

	public static ConfigConverter<InetSocketAddress> ofInetSocketAddress() {
		return new SimpleConfigConverter<InetSocketAddress>() {
			@Override
			public InetSocketAddress fromString(String addressPort) {
				int portPos = addressPort.lastIndexOf(':');
				if (portPos == -1) {
					return new InetSocketAddress(Integer.parseInt(addressPort));
				}
				String addressStr = addressPort.substring(0, portPos);
				String portStr = addressPort.substring(portPos + 1);
				int port = parseInt(portStr);
				checkArgument(port > 0 && port < 65536, "Invalid address. Port is not in range (0, 65536) " + addressStr);
				InetSocketAddress socketAddress;
				if ("*".equals(addressStr)) {
					socketAddress = new InetSocketAddress(port);
				} else {
					try {
						InetAddress address = InetAddress.getByName(addressStr);
						socketAddress = new InetSocketAddress(address, port);
					} catch (UnknownHostException e) {
						throw new IllegalArgumentException(e);
					}
				}
				return socketAddress;
			}

			@Override
			public String toString(InetSocketAddress item) {
				return item.getAddress().getHostAddress() + ":" + item.getPort();
			}
		};
	}

	public static ConfigConverter<Path> ofPath() {
		return new SimpleConfigConverter<Path>() {
			@Override
			protected Path fromString(String value) {
				return Paths.get(value);
			}

			@Override
			protected String toString(Path defaultValue) {
				return defaultValue.toAbsolutePath().normalize().toString();
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
			public String toString(MemSize item) {
				return item.format();
			}
		};
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
			public String toString(InetAddressRange item) {
				return item.toString();
			}
		};
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverter<T> elementConverter, CharSequence separators) {
		return new SimpleConfigConverter<List<T>>() {
			private final Pattern pattern = compile(separators.chars()
					.mapToObj(c -> "\\" + ((char) c))
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
			public String toString(List<T> item) {
				return item.stream()
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
						.andThen(applyNotNull(
								ServerSocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? MemSize.of(defaultValue.getReceiveBufferSize()) : null)))
						.andThen(applyNotNull(
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
						.andThen(applyNotNull(
								SocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? MemSize.of(defaultValue.getReceiveBufferSize()) : null)))
						.andThen(applyNotNull(
								SocketSettings::withSendBufferSize,
								config.get(ofMemSize(), "sendBufferSize",
										defaultValue.hasSendBufferSize() ? MemSize.of(defaultValue.getSendBufferSize()) : null)))
						.andThen(applyNotNull(
								SocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.andThen(applyNotNull(
								SocketSettings::withKeepAlive,
								config.get(ofBoolean(), "keepAlive",
										defaultValue.hasKeepAlive() ? defaultValue.getKeepAlive() : null)))
						.andThen(applyNotNull(
								SocketSettings::withTcpNoDelay,
								config.get(ofBoolean(), "tcpNoDelay",
										defaultValue.hasTcpNoDelay() ? defaultValue.getTcpNoDelay() : null)))
						.andThen(applyNotNull(
								SocketSettings::withImplReadTimeout,
								config.get(ofLong(), "implReadTimeout",
										defaultValue.hasImplReadTimeout() ? defaultValue.getImplReadTimeout() : null)))
						.andThen(applyNotNull(
								SocketSettings::withImplWriteTimeout,
								config.get(ofLong(), "implWriteTimeout",
										defaultValue.hasImplWriteTimeout() ? defaultValue.getImplWriteTimeout() : null)))
						.andThen(applyNotNull(
								SocketSettings::withImplReadSize,
								config.get(ofMemSize(), "implReadSize",
										defaultValue.hasImplReadSize() ? MemSize.of(defaultValue.getImplReadSize()) : null)))
						.andThen(applyNotNull(
								SocketSettings::withImplWriteSize,
								config.get(ofMemSize(), "implWriteSize",
										defaultValue.hasImplWriteSize() ? MemSize.of(defaultValue.getImplWriteSize()) : null)))
						.apply(SocketSettings.create());
			}
		};
	}

	public static ConfigConverter<DatagramSocketSettings> ofDatagramSocketSettings() {
		return new ComplexConfigConverter<DatagramSocketSettings>(DatagramSocketSettings.create()) {
			@Override
			protected DatagramSocketSettings provide(Config config, DatagramSocketSettings defaultValue) {
				return Function.<DatagramSocketSettings>identity()
						.andThen(applyNotNull(
								DatagramSocketSettings::withReceiveBufferSize,
								config.get(ofMemSize(), "receiveBufferSize",
										defaultValue.hasReceiveBufferSize() ? MemSize.of(defaultValue.getReceiveBufferSize()) : null)))
						.andThen(applyNotNull(
								DatagramSocketSettings::withSendBufferSize,
								config.get(ofMemSize(), "sendBufferSize",
										defaultValue.hasSendBufferSize() ? MemSize.of(defaultValue.getSendBufferSize()) : null)))
						.andThen(applyNotNull(
								DatagramSocketSettings::withReuseAddress,
								config.get(ofBoolean(), "reuseAddress",
										defaultValue.hasReuseAddress() ? defaultValue.getReuseAddress() : null)))
						.andThen(applyNotNull(
								DatagramSocketSettings::withBroadcast,
								config.get(ofBoolean(), "broadcast",
										defaultValue.hasBroadcast() ? defaultValue.getBroadcast() : null)))
						.apply(DatagramSocketSettings.create());
			}
		};
	}

	public static final ConfigConverter<List<Class>> OF_CLASSES = ofList(ofClass());

	public static ConfigConverter<FatalErrorHandler> ofFatalErrorHandler() {
		return new ConfigConverter<FatalErrorHandler>() {
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

	public static ConfigConverter<ThrottlingController> ofThrottlingController() {
		return new ComplexConfigConverter<ThrottlingController>(ThrottlingController.create()) {
			@Override
			protected ThrottlingController provide(Config config, ThrottlingController defaultValue) {
				return ThrottlingController.create()
						.withTargetTimeMillis(config.get(ofInteger(), "targetTimeMillis", defaultValue.getTargetTimeMillis()))
						.withGcTimeMillis(config.get(ofInteger(), "gcTimeMillis", defaultValue.getGcTimeMillis()))
						.withSmoothingWindow(config.get(ofInteger(), "smoothingWindow", defaultValue.getSmoothingWindow()))
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
				String threadGroupName = config.get(ofNullableString(), "threadGroup", transform(defaultValue.getThreadGroup(), ThreadGroup::getName));
				if (threadGroupName != null) {
					result = result.withThreadGroup(new ThreadGroup(threadGroupName));
				}
				return result
						.withName(config.get(ofNullableString(), "name", defaultValue.getName()))
						.withPriority(config.get(ofInteger(), "priority", defaultValue.getPriority()))
						.withDaemon(config.get(ofBoolean(), "daemon", defaultValue.isDaemon()));
			}
		};
	}

	public static ConfigConverter<HikariConfig> ofHikariConfig() {
		HikariConfig defaultValue = new HikariConfig();
		defaultValue.setRegisterMbeans(true);
		return new ComplexConfigConverter<HikariConfig>(defaultValue) {
			@Override
			protected HikariConfig provide(Config c, HikariConfig d) {
				HikariConfig r = new HikariConfig();
				c.apply(ofBoolean(), "autoCommit", d.isAutoCommit(), r::setAutoCommit);
				c.apply(ofString(), "catalog", d.getCatalog(), r::setCatalog);
				c.apply(ofString(), "connectionInitSql", d.getConnectionInitSql(), r::setConnectionInitSql);
				c.apply(ofString(), "connectionTestQuery", d.getConnectionTestQuery(), r::setConnectionTestQuery);
				c.apply(ofLong(), "connectionTimeout", d.getConnectionTimeout(), r::setConnectionTimeout);
				c.apply(ofString(), "dataSourceClassName", d.getDataSourceClassName(), r::setDataSourceClassName);
				c.apply(ofString(), "driverClassName", d.getDriverClassName(), ifNotDefault(r::setDriverClassName));
				c.apply(ofLong(), "idleTimeout", d.getIdleTimeout(), r::setIdleTimeout);
				c.apply(ofBoolean(), "initializationFailFast", d.isInitializationFailFast(), r::setInitializationFailFast);
				c.apply(ofBoolean(), "isolateInternalQueries", d.isIsolateInternalQueries(), r::setIsolateInternalQueries);
				c.apply(ofString(), "jdbcUrl", d.getJdbcUrl(), r::setJdbcUrl);
				c.apply(ofLong(), "leakDetectionThreshold", d.getLeakDetectionThreshold(), r::setLeakDetectionThreshold);
				c.apply(ofInteger(), "maximumPoolSize", d.getMaximumPoolSize(), r::setMaximumPoolSize);
				c.apply(ofLong(), "maxLifetime", d.getMaxLifetime(), r::setMaxLifetime);
				c.apply(ofInteger(), "minimumIdle", d.getMinimumIdle(), ifNotDefault(r::setMinimumIdle));
				c.apply(ofString(), "password", d.getPassword(), r::setPassword);
				c.apply(ofString(), "poolName", d.getPoolName(), r::setPoolName);
				c.apply(ofBoolean(), "readOnly", d.isReadOnly(), r::setReadOnly);
				c.apply(ofBoolean(), "registerMbeans", d.isRegisterMbeans(), r::setRegisterMbeans);
				c.apply(ofString(), "transactionIsolation", d.getTransactionIsolation(), r::setTransactionIsolation);
				c.apply(ofString(), "username", d.getUsername(), r::setUsername);
				c.apply(ofThreadFactory(), "threadFactory", null, ifNotNull(r::setThreadFactory));
				Config propertiesConfig = c.getChild("extra");
				for (String property : propertiesConfig.getChildren().keySet()) {
					String value = propertiesConfig.get(property);
					d.addDataSourceProperty(property, value);
				}
				return r;
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
			@Override
			public ExecutorService get(Config config) {
				return getExecutor(config);
			}

			@Override
			public ExecutorService get(Config config, ExecutorService defaultValue) {
				throw new UnsupportedOperationException();
			}
		};
	}

}
