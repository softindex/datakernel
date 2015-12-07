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

import io.datakernel.util.Joiner;
import io.datakernel.util.Splitter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;

public final class ConfigConverters {

	private static final class ConfigConverterString extends ConfigConverterSingle<String> {
		static ConfigConverterString INSTANCE = new ConfigConverterString();

		@Override
		public String fromString(String string) {
			return checkNotNull(string);
		}

		@Override
		public String toString(String item) {
			return checkNotNull(item);
		}
	}

	private static final class ConfigConverterInteger extends ConfigConverterSingle<Integer> {
		static ConfigConverterInteger INSTANCE = new ConfigConverterInteger();

		@Override
		public Integer fromString(String string) {
			return Integer.parseInt(string);
		}

		@Override
		public String toString(Integer item) {
			return Integer.toString(item);
		}
	}

	private static final class ConfigConverterLong extends ConfigConverterSingle<Long> {
		static ConfigConverterLong INSTANCE = new ConfigConverterLong();

		@Override
		public Long fromString(String string) {
			return Long.parseLong(string);
		}

		@Override
		public String toString(Long item) {
			return Long.toString(item);
		}
	}

	private static final class ConfigConverterDouble extends ConfigConverterSingle<Double> {
		static ConfigConverterDouble INSTANCE = new ConfigConverterDouble();

		@Override
		public Double fromString(String string) {
			return Double.parseDouble(string);
		}

		@Override
		public String toString(Double item) {
			return Double.toString(item);
		}
	}

	private static final class ConfigConverterBoolean extends ConfigConverterSingle<Boolean> {
		static ConfigConverterBoolean INSTANCE = new ConfigConverterBoolean();

		@Override
		public Boolean fromString(String string) {
			return Boolean.parseBoolean(string);
		}

		@Override
		public String toString(Boolean item) {
			return Boolean.toString(item);
		}
	}

	private static final class ConfigConverterEnum<E extends Enum<E>> extends ConfigConverterSingle<E> {
		private final Class<E> enumClass;

		public ConfigConverterEnum(Class<E> enumClass) {
			this.enumClass = enumClass;
		}

		@Override
		protected E fromString(String string) {
			return Enum.valueOf(enumClass, string);
		}

		@Override
		protected String toString(E item) {
			return item.name();
		}
	}

	private static final class ConfigConverterInetSocketAddress extends ConfigConverterSingle<InetSocketAddress> {
		static ConfigConverterInetSocketAddress INSTANCE = new ConfigConverterInetSocketAddress();

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
	}

	private static final class ConfigConverterList<T> extends ConfigConverterSingle<List<T>> {
		private final ConfigConverterSingle<T> elementConverter;
		private final Splitter splitter;
		private final Joiner joiner;

		public ConfigConverterList(ConfigConverterSingle<T> elementConverter, CharSequence separators) {
			this.elementConverter = elementConverter;
			this.splitter = Splitter.onAnyOf(separators);
			this.joiner = Joiner.on(separators.charAt(0));
		}

		@Override
		protected List<T> fromString(String string) {
			string = string.trim();
			if (string.isEmpty())
				return emptyList();
			List<T> list = new ArrayList<>();
			for (String elementString : splitter.split(string)) {
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
			return joiner.join(strings);
		}
	}

	public static ConfigConverterSingle<String> ofString() {
		return ConfigConverterString.INSTANCE;
	}

	public static ConfigConverterSingle<Integer> ofInteger() {
		return ConfigConverterInteger.INSTANCE;
	}

	public static ConfigConverterSingle<Long> ofLong() {
		return ConfigConverterLong.INSTANCE;
	}

	public static ConfigConverterSingle<Double> ofDouble() {
		return ConfigConverterDouble.INSTANCE;
	}

	public static ConfigConverterSingle<Boolean> ofBoolean() {
		return ConfigConverterBoolean.INSTANCE;
	}

	public static <E extends Enum<E>> ConfigConverterSingle<E> ofEnum(Class<E> enumClass) {
		return new ConfigConverterEnum<>(enumClass);
	}

	public static ConfigConverterSingle<InetSocketAddress> ofInetSocketAddress() {
		return ConfigConverterInetSocketAddress.INSTANCE;
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverterSingle<T> elementConverter, CharSequence separators) {
		return new ConfigConverterList<>(elementConverter, separators);
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverterSingle<T> elementConverter) {
		return ofList(elementConverter, ",;");
	}

}

