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

import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.exception.ParseException;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.Preconditions;
import io.datakernel.util.StringUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;

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
		final Class<E> enumClass1 = enumClass;
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
				checkArgument(portPos != -1, "Invalid address. Port is not specified");
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
		final AbstractConfigConverter<T> elementConverter1 = elementConverter;
		final CharSequence separators1 = separators;
		return new AbstractConfigConverter<List<T>>() {
			private final AbstractConfigConverter<T> elementConverter = elementConverter1;
			private final CharSequence separators = separators1;
			private final char joinSeparator = separators1.charAt(0);

			@Override
			public List<T> fromString(String string) {
				string = string.trim();
				if (string.isEmpty())
					return emptyList();
				List<T> list = new ArrayList<>();
				for (String elementString : StringUtils.splitToList(separators, string)) {
					T element = elementConverter.fromString(elementString.trim());
					list.add(element);
				}
				return Collections.unmodifiableList(list);
			}

			@Override
			public String toString(List<T> item) {
				List<String> strings = new ArrayList<>(item.size());
				for (T e : item) {
					strings.add(elementConverter.toString(e));
				}
				return StringUtils.join(joinSeparator, strings);
			}
		};
	}

	public static <T> AbstractConfigConverter<List<T>> ofList(AbstractConfigConverter<T> elementConverter) {
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
				DatagramSocketSettings result = Preconditions.checkNotNull(DatagramSocketSettings.create());

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
}
