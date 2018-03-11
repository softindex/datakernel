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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static io.datakernel.config.Config.THIS;
import static io.datakernel.config.Config.ifNotDefault;
import static io.datakernel.eventloop.FatalErrorHandlers.*;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_KEYS_PER_SECOND;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_THROTTLING;
import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
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
				return config.get(THIS, defaultValue);
			}

			@Override
			public String get(Config config) {
				return get(config, "");
			}
		};
	}

	public static ConfigConverter<String> ofNullableString() {
		return new AbstractConfigConverter<String>() {
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
		return new AbstractConfigConverter<Byte>() {
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
		return new AbstractConfigConverter<Integer>() {
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
		return new AbstractConfigConverter<Long>() {
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
		return new AbstractConfigConverter<Float>() {
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
		return new AbstractConfigConverter<Double>() {
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
		return new AbstractConfigConverter<Boolean>() {
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

	public static <E extends Enum<E>> AbstractConfigConverter<E> ofEnum(Class<E> enumClass) {
		Class<E> enumClass1 = enumClass;
		return new AbstractConfigConverter<E>() {
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
		return new AbstractConfigConverter<Class>() {
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
		return new AbstractConfigConverter<InetAddress>() {
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
		return new AbstractConfigConverter<InetSocketAddress>() {
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
		return new AbstractConfigConverter<Path>() {
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
		return new AbstractConfigConverter<MemSize>() {
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
		return new AbstractConfigConverter<InetAddressRange>() {
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
		return new AbstractConfigConverter<List<T>>() {
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
							return config.get(THIS);
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
		return new ConfigConverter<ServerSocketSettings>() {
			@Override
			public ServerSocketSettings get(Config config) {
				return get(config, ServerSocketSettings.create(DEFAULT_BACKLOG));
			}

			@Override
			public ServerSocketSettings get(Config config, ServerSocketSettings defaultValue) {
				ServerSocketSettings result = checkNotNull(defaultValue);

				result = result.withBacklog(config.get(ofInteger(), "backlog", result.getBacklog()));

				MemSize receiveBufferSize = config.get(ofMemSize(), "receiveBufferSize",
						result.hasReceiveBufferSize() ? MemSize.of(result.getReceiveBufferSize()) : null);
				if (receiveBufferSize != null) {
					result = result.withReceiveBufferSize(receiveBufferSize);
				}

				Boolean reuseAddress = config.get(ofBoolean(), "reuseAddress",
						result.hasReuseAddress() ? result.getReuseAddress() : null);
				if (reuseAddress != null) {
					result = result.withReuseAddress(reuseAddress);
				}

				return result;
			}
		};
	}

	public static ConfigConverter<SocketSettings> ofSocketSettings() {
		return new ConfigConverter<SocketSettings>() {
			@Override
			public SocketSettings get(Config config) {
				return get(config, SocketSettings.create());
			}

			@Override
			public SocketSettings get(Config config, SocketSettings defaultValue) {
				SocketSettings result = checkNotNull(defaultValue);

				MemSize receiveBufferSize = config.get(ofMemSize(), "receiveBufferSize",
						result.hasReceiveBufferSize() ? MemSize.of(result.getReceiveBufferSize()) : null);
				if (receiveBufferSize != null) {
					result = result.withReceiveBufferSize(receiveBufferSize);
				}

				MemSize sendBufferSize = config.get(ofMemSize(), "sendBufferSize",
						result.hasSendBufferSize() ? MemSize.of(result.getSendBufferSize()) : null);
				if (sendBufferSize != null) {
					result = result.withSendBufferSize(sendBufferSize);
				}

				Boolean reuseAddress = config.get(ofBoolean(), "reuseAddress",
						result.hasReuseAddress() ? result.getReuseAddress() : null);
				if (reuseAddress != null) {
					result = result.withReuseAddress(reuseAddress);
				}

				Boolean keepAlive = config.get(ofBoolean(), "keepAlive",
						result.hasKeepAlive() ? result.getKeepAlive() : null);
				if (keepAlive != null) {
					result = result.withKeepAlive(keepAlive);
				}

				Boolean tcpNoDelay = config.get(ofBoolean(), "tcpNoDelay",
						result.hasTcpNoDelay() ? result.getTcpNoDelay() : null);
				if (tcpNoDelay != null) {
					result = result.withTcpNoDelay(tcpNoDelay);
				}

				Long implReadTimeout = config.get(ofLong(), "implReadTimeout",
						result.hasImplReadTimeout() ? result.getImplReadTimeout() : null);
				if (implReadTimeout != null) {
					result = result.withImplReadTimeout(implReadTimeout);
				}

				Long implWriteTimeout = config.get(ofLong(), "implWriteTimeout",
						result.hasImplWriteTimeout() ? result.getImplWriteTimeout() : null);
				if (implWriteTimeout != null) {
					result = result.withImplWriteTimeout(implWriteTimeout);
				}

				MemSize implReadSize = config.get(ofMemSize(), "implReadSize",
						result.hasImplReadSize() ? MemSize.of(result.getImplReadSize()) : null);
				if (implReadSize != null) {
					result = result.withImplReadSize(implReadSize);
				}

				MemSize implWriteSize = config.get(ofMemSize(), "implWriteSize",
						result.hasImplWriteSize() ? MemSize.of(result.getImplWriteSize()) : null);
				if (implWriteSize != null) {
					result = result.withImplWriteSize(implWriteSize);
				}

				return result;
			}
		};
	}

	public static ConfigConverter<DatagramSocketSettings> ofDatagramSocketSettings() {
		return new ConfigConverter<DatagramSocketSettings>() {
			@Override
			public DatagramSocketSettings get(Config config) {
				return get(config, DatagramSocketSettings.create());
			}

			@Override
			public DatagramSocketSettings get(Config config, DatagramSocketSettings defaultValue) {
				DatagramSocketSettings result = checkNotNull(defaultValue);

				MemSize receiveBufferSize = config.get(ofMemSize(), "receiveBufferSize", result.hasReceiveBufferSize() ?
						MemSize.of(result.getReceiveBufferSize()) : null);
				if (receiveBufferSize != null) {
					result = result.withReceiveBufferSize(receiveBufferSize);
				}

				MemSize sendBufferSize = config.get(ofMemSize(), "sendBufferSize", result.hasSendBufferSize() ?
						MemSize.of(result.getSendBufferSize()) : null);
				if (sendBufferSize != null) {
					result = result.withSendBufferSize(sendBufferSize);
				}

				Boolean reuseAddress = config.get(ofBoolean(), "reuseAddress", result.hasReuseAddress() ?
						result.getReuseAddress() : null);
				if (reuseAddress != null) {
					result = result.withReuseAddress(reuseAddress);
				}

				Boolean broadcast = config.get(ofBoolean(), "broadcast", result.hasBroadcast() ?
						result.getBroadcast() : null);
				if (broadcast != null) {
					result = result.withBroadcast(broadcast);
				}

				return result;
			}
		};
	}

	private static final Map<String, FatalErrorHandler> simpleErrorHandlers = new HashMap<>();

	static {
		simpleErrorHandlers.put("rethrowOnAnyError", rethrowOnAnyError());
		simpleErrorHandlers.put("ignoreAllErrors", ignoreAllErrors());
		simpleErrorHandlers.put("exitOnAnyError", exitOnAnyError());
		simpleErrorHandlers.put("exitOnJvmError", exitOnJvmError());
	}

	public static ConfigConverter<FatalErrorHandler> ofFatalErrorHandler() {
		return new ConfigConverter<FatalErrorHandler>() {
			@Override
			public FatalErrorHandler get(Config config, FatalErrorHandler defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				}
				return get(config);
			}

			@Override
			public FatalErrorHandler get(Config config) {
				String key = config.get(THIS);

				ConfigConverter<List<Class>> classList = ofList(ofClass());
				switch (key) {
					case "rethrowOnMatchedError":
						return rethrowOnMatchedError(
								config.get(classList, "whitelist", emptyList()),
								config.get(classList, "blacklist", emptyList()));
					case "exitOnMatchedError":
						return exitOnMatchedError(
								config.get(classList, "whitelist", emptyList()),
								config.get(classList, "blacklist", emptyList()));
					default:
						FatalErrorHandler fatalErrorHandler = simpleErrorHandlers.get(key);
						if (fatalErrorHandler == null) {
							throw new IllegalArgumentException("No fatal error handler named " + key + " exists!");
						}
						return fatalErrorHandler;
				}
			}
		};
	}

	public static ConfigConverter<ThrottlingController> ofThrottlingController() {
		return new ConfigConverter<ThrottlingController>() {
			@Override
			public ThrottlingController get(Config config) {
				return get(config, ThrottlingController.create());
			}

			@Override
			public ThrottlingController get(Config config, ThrottlingController defaultValue) {
				checkNotNull(defaultValue);
				return defaultValue
						.withTargetTimeMillis(config.get(ofInteger(), "targetTimeMillis", defaultValue.getTargetTimeMillis()))
						.withGcTimeMillis(config.get(ofInteger(), "gcTimeMillis", defaultValue.getGcTimeMillis()))
						.withSmoothingWindow(config.get(ofInteger(), "smoothingWindow", defaultValue.getSmoothingWindow()))
						.withThrottlingDecrease(config.get(ofDouble(), "throttlingDecrease", defaultValue.getThrottlingDecrease()))
						.withInitialKeysPerSecond(config.get(ofDouble(), "initialKeysPerSecond", INITIAL_KEYS_PER_SECOND))
						.withInitialThrottling(config.get(ofDouble(), "initialThrottling", INITIAL_THROTTLING));
			}
		};
	}

	public static ConfigConverter<HikariConfig> ofHikariConfig() {
		return new ConfigConverter<HikariConfig>() {
			@Override
			public HikariConfig get(Config config) {
				return get(config, new HikariConfig());
			}

			@Override
			public HikariConfig get(Config config, HikariConfig c) {
				checkNotNull(c);
				config.apply(ofBoolean(), "autoCommit", c.isAutoCommit(), c::setAutoCommit);
				config.apply(ofString(), "catalog", c.getCatalog(), c::setCatalog);
				config.apply(ofString(), "connectionInitSql", c.getConnectionInitSql(), c::setConnectionInitSql);
				config.apply(ofString(), "connectionTestQuery", c.getConnectionTestQuery(), c::setConnectionTestQuery);
				config.apply(ofLong(), "connectionTimeout", c.getConnectionTimeout(), c::setConnectionTimeout);
				config.apply(ofString(), "dataSourceClassName", c.getDataSourceClassName(), c::setDataSourceClassName);
				config.apply(ofString(), "driverClassName", c.getDriverClassName(), ifNotDefault(c::setDriverClassName));
				config.apply(ofLong(), "idleTimeout", c.getIdleTimeout(), c::setIdleTimeout);
				config.apply(ofBoolean(), "initializationFailFast", c.isInitializationFailFast(), c::setInitializationFailFast);
				config.apply(ofBoolean(), "isolateInternalQueries", c.isIsolateInternalQueries(), c::setIsolateInternalQueries);
				config.apply(ofString(), "jdbcUrl", c.getJdbcUrl(), c::setJdbcUrl);
				config.apply(ofLong(), "leakDetectionThreshold", c.getLeakDetectionThreshold(), c::setLeakDetectionThreshold);
				config.apply(ofInteger(), "maximumPoolSize", c.getMaximumPoolSize(), c::setMaximumPoolSize);
				config.apply(ofLong(), "maxLifetime", c.getMaxLifetime(), c::setMaxLifetime);
				config.apply(ofInteger(), "minimumIdle", c.getMinimumIdle(), ifNotDefault(c::setMinimumIdle));
				config.apply(ofString(), "password", c.getPassword(), c::setPassword);
				config.apply(ofString(), "poolName", c.getPoolName(), c::setPoolName);
				config.apply(ofBoolean(), "readOnly", c.isReadOnly(), c::setReadOnly);
				config.apply(ofBoolean(), "registerMbeans", c.isRegisterMbeans(), c::setRegisterMbeans);
				config.apply(ofString(), "transactionIsolation", c.getTransactionIsolation(), c::setTransactionIsolation);
				config.apply(ofString(), "username", c.getUsername(), c::setUsername);
				Config propertiesConfig = config.getChild("extra");
				for (String property : propertiesConfig.getChildren().keySet()) {
					String value = propertiesConfig.get(property);
					c.addDataSourceProperty(property, value);
				}
				return c;
			}
		};
	}

	public static ExecutorService getExecutorService(Config config) {
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
}
