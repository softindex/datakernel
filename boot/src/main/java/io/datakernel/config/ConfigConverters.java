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

import io.datakernel.eventloop.FatalErrorHandler;
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.exception.ParseException;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.Preconditions;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.datakernel.eventloop.FatalErrorHandlers.*;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_KEYS_PER_SECOND;
import static io.datakernel.eventloop.ThrottlingController.INITIAL_THROTTLING;
import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"unused", "WeakerAccess"})
public final class ConfigConverters {
	private ConfigConverters() {
	}

	// simple
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

	public static AbstractConfigConverter<Integer> ofInteger() {
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

	public static AbstractConfigConverter<Long> ofLong() {
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

	public static AbstractConfigConverter<Float> ofFloat() {
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

	public static AbstractConfigConverter<Double> ofDouble() {
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

	public static AbstractConfigConverter<Boolean> ofBoolean() {
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

	public static AbstractConfigConverter<Class> ofClass() {
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

	public static AbstractConfigConverter<InetAddress> ofInetAddress() {
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

	public static AbstractConfigConverter<InetSocketAddress> ofInetSocketAddress() {
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

	public static AbstractConfigConverter<Path> ofPath() {
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

	public static AbstractConfigConverter<MemSize> ofMemSize() {
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

	public static AbstractConfigConverter<InetAddressRange> ofInetAddressRange() {
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

	public static <T> AbstractConfigConverter<List<T>> ofList(AbstractConfigConverter<T> elementConverter, CharSequence separators) {
		return new AbstractConfigConverter<List<T>>() {
			private final Pattern pattern = compile(separators.chars()
					.mapToObj(c -> "\\" + ((char) c))
					.collect(joining("", "[", "]")));

			@Override
			public List<T> fromString(String string) {
				return pattern.splitAsStream(string)
						.map(String::trim)
						.filter(s -> !s.isEmpty())
						.map(elementConverter::fromString)
						.collect(toList());
			}

			@Override
			public String toString(List<T> item) {
				return item.stream()
						.map(elementConverter::toString)
						.collect(joining(String.valueOf(separators.charAt(0))));
			}
		};
	}

	public static <T> AbstractConfigConverter<List<T>> ofList(AbstractConfigConverter<T> elementConverter) {
		return ofList(elementConverter, ",;");
	}

	public static <T> ConfigConverter<T> constrain(ConfigConverter<T> converter, Predicate<T> predicate) {
		return new ConfigConverter<T>() {

			@Override
			public T get(Config config, T defaultValue) {
				T t = converter.get(config, defaultValue);
				checkArgument(predicate.test(t), "Value " + t + " does not satisfy the constraint for " + config);
				return t;
			}

			@Override
			public T get(Config config) {
				T t = converter.get(config);
				checkArgument(predicate.test(t), "Value " + t + " does not satisfy the constraint for " + config);
				return t;
			}
		};
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
				ServerSocketSettings result = Preconditions.checkNotNull(defaultValue);

				result = result.withBacklog(config.get(ofInteger(), "backlog", result.getBacklog()));

				MemSize receiveBufferSize = config.get(ofMemSize(), "receiveBufferSize", result.hasReceiveBufferSize() ?
						MemSize.of(result.getReceiveBufferSize()) : null);
				if (receiveBufferSize != null) {
					result = result.withReceiveBufferSize(receiveBufferSize);
				}

				Boolean reuseAddress = config.get(ofBoolean(), "reuseAddress", result.hasReuseAddress() ?
						result.getReuseAddress() : null);
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
				SocketSettings result = Preconditions.checkNotNull(defaultValue);

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

				Boolean keepAlive = config.get(ofBoolean(), "keepAlive", result.hasKeepAlive() ?
						result.getKeepAlive() : null);
				if (keepAlive != null) {
					result = result.withKeepAlive(keepAlive);
				}

				Boolean tcpNoDelay = config.get(ofBoolean(), "tcpNoDelay", result.hasTcpNoDelay() ?
						result.getTcpNoDelay() : null);
				if (tcpNoDelay != null) {
					result = result.withTcpNoDelay(tcpNoDelay);
				}

				Long implReadTimeout = config.get(ofLong(), "implReadTimeout", result.hasImplReadTimeout() ?
						result.getImplReadTimeout() : null);
				if (implReadTimeout != null) {
					result = result.withImplReadTimeout(implReadTimeout);
				}

				Long implWriteTimeout = config.get(ofLong(), "implWriteTimeout", result.hasImplWriteTimeout() ?
						result.getImplWriteTimeout() : null);
				if (implWriteTimeout != null) {
					result = result.withImplWriteTimeout(implWriteTimeout);
				}

				MemSize implReadSize = config.get(ofMemSize(), "implReadSize", result.hasImplReadSize() ?
						MemSize.of(result.getImplReadSize()) : null);
				if (implReadSize != null) {
					result = result.withImplReadSize(implReadSize);
				}

				MemSize implWriteSize = config.get(ofMemSize(), "implWriteSize", result.hasImplWriteSize() ?
						MemSize.of(result.getImplWriteSize()) : null);
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
				DatagramSocketSettings result = Preconditions.checkNotNull(defaultValue);

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
				String key = config.getValue();

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
				Preconditions.checkNotNull(defaultValue);
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

}
