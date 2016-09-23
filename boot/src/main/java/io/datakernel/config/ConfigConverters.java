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

import com.google.common.base.Preconditions;
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.exception.ParseException;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.StringUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.datakernel.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;

@SuppressWarnings("unused, WeakerAccess")
public final class ConfigConverters {

	public static ConfigConverterSingle<String> ofString() {
		return new ConfigConverterSingle<String>() {
			@Override
			public String fromString(String string) {
				return checkNotNull(string);
			}

			@Override
			public String toString(String item) {
				return checkNotNull(item);
			}
		};
	}

	public static ConfigConverterSingle<Byte> ofByte() {
		return new ConfigConverterSingle<Byte>() {
			@Override
			public Byte fromString(String string) {
				return Byte.parseByte(string);
			}

			@Override
			public String toString(Byte item) {
				return Byte.toString(item);
			}
		};
	}

	public static ConfigConverterSingle<Integer> ofInteger() {
		return new ConfigConverterSingle<Integer>() {
			@Override
			public Integer fromString(String string) {
				return Integer.parseInt(string);
			}

			@Override
			public String toString(Integer item) {
				return Integer.toString(item);
			}
		};
	}

	public static ConfigConverterSingle<Long> ofLong() {
		return new ConfigConverterSingle<Long>() {
			@Override
			public Long fromString(String string) {
				return Long.parseLong(string);
			}

			@Override
			public String toString(Long item) {
				return Long.toString(item);
			}
		};
	}

	public static ConfigConverterSingle<Float> ofFloat() {
		return new ConfigConverterSingle<Float>() {
			@Override
			public Float fromString(String string) {
				return Float.parseFloat(string);
			}

			@Override
			public String toString(Float item) {
				return Float.toString(item);
			}
		};
	}

	public static ConfigConverterSingle<Double> ofDouble() {
		return new ConfigConverterSingle<Double>() {
			@Override
			public Double fromString(String string) {
				return Double.parseDouble(string);
			}

			@Override
			public String toString(Double item) {
				return Double.toString(item);
			}
		};
	}

	public static ConfigConverterSingle<Boolean> ofBoolean() {
		return new ConfigConverterSingle<Boolean>() {
			@Override
			public Boolean fromString(String string) {
				return Boolean.parseBoolean(string);
			}

			@Override
			public String toString(Boolean item) {
				return Boolean.toString(item);
			}
		};
	}

	public static <E extends Enum<E>> ConfigConverterSingle<E> ofEnum(Class<E> enumClass) {
		final Class<E> enumClass1 = enumClass;
		return new ConfigConverterSingle<E>() {
			private final Class<E> enumClass = enumClass1;

			@Override
			protected E fromString(String string) {
				return Enum.valueOf(enumClass, string);
			}

			@Override
			protected String toString(E item) {
				return item.name();
			}
		};
	}

	public static ConfigConverterSingle<InetAddress> ofInetAddress() {
		return new ConfigConverterSingle<InetAddress>() {
			@Override
			protected InetAddress fromString(String address) {
				try {
					return InetAddress.getByName(address);
				} catch (UnknownHostException e) {
					throw new IllegalArgumentException(e);
				}
			}

			@Override
			protected String toString(InetAddress item) {
				return item.getAddress().toString();
			}
		};
	}

	public static ConfigConverterSingle<InetSocketAddress> ofInetSocketAddress() {
		return new ConfigConverterSingle<InetSocketAddress>() {
			@Override
			protected InetSocketAddress fromString(String addressPort) {
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
			protected String toString(InetSocketAddress item) {
				return item.getAddress().getHostAddress() + ":" + item.getPort();
			}
		};
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverterSingle<T> elementConverter, CharSequence separators) {
		final ConfigConverterSingle<T> elementConverter1 = elementConverter;
		final CharSequence separators1 = separators;
		return new ConfigConverterSingle<List<T>>() {
			private final ConfigConverterSingle<T> elementConverter = elementConverter1;
			private final CharSequence separators = separators1;
			private final char joinSeparator = separators1.charAt(0);

			@Override
			protected List<T> fromString(String string) {
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
			protected String toString(List<T> item) {
				List<String> strings = new ArrayList<>(item.size());
				for (T e : item) {
					strings.add(elementConverter.toString(e));
				}
				return StringUtils.join(joinSeparator, strings);
			}
		};
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverterSingle<T> elementConverter) {
		return ofList(elementConverter, ",;");
	}

	public static ConfigConverterSingle<MemSize> ofMemSize() {
		return new ConfigConverterSingle<MemSize>() {
			@Override
			protected MemSize fromString(String string) {
				return MemSize.valueOf(string);
			}

			@Override
			protected String toString(MemSize item) {
				return item.format();
			}
		};
	}

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
				if (config.hasValue("receiveBufferSize"))
					result = result.withReceiveBufferSize(config.get(ofMemSize(), "receiveBufferSize"));
				if (config.hasValue("reuseAddress"))
					result = result.withReuseAddress(config.get(ofBoolean(), "reuseAddress"));
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
				if (config.hasValue("receiveBufferSize"))
					result = result.withReceiveBufferSize(config.get(ofMemSize(), "receiveBufferSize"));
				if (config.hasValue("sendBufferSize"))
					result = result.withSendBufferSize(config.get(ofMemSize(), "sendBufferSize"));
				if (config.hasValue("reuseAddress"))
					result = result.withReuseAddress(config.get(ofBoolean(), "reuseAddress"));
				if (config.hasValue("keepAlive"))
					result = result.withKeepAlive(config.get(ofBoolean(), "keepAlive"));
				if (config.hasValue("tcpNoDelay"))
					result = result.withTcpNoDelay(config.get(ofBoolean(), "tcpNoDelay"));
				if (config.hasValue("readTimeout"))
					result = result.withReadTimeout(config.get(ofLong(), "readTimeout"));
				if (config.hasValue("writeTimeout"))
					result = result.withWriteTimeout(config.get(ofLong(), "writeTimeout"));
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
				if (config.hasValue("receiveBufferSize"))
					result = result.withReceiveBufferSize(config.get(ofMemSize(), "receiveBufferSize"));
				if (config.hasValue("sendBufferSize"))
					result = result.withSendBufferSize(config.get(ofMemSize(), "sendBufferSize"));
				if (config.hasValue("reuseAddress"))
					result = result.withReuseAddress(config.get(ofBoolean(), "reuseAddress"));
				if (config.hasValue("broadcast"))
					result = result.withBroadcast(config.get(ofBoolean(), "broadcast"));
				return result;
			}
		};
	}

	public static ConfigConverterSingle<InetAddressRange> ofInetAddressRange() {
		return new ConfigConverterSingle<InetAddressRange>() {
			@Override
			protected InetAddressRange fromString(String string) {
				try {
					return InetAddressRange.parse(string);
				} catch (ParseException e) {
					throw new IllegalArgumentException("Can't parse inetAddressRange config", e);
				}
			}

			@Override
			protected String toString(InetAddressRange item) {
				return item.toString();
			}
		};
	}
}

