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

package io.datakernel.common;

import io.datakernel.common.parse.ParseException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.*;

import static io.datakernel.common.Preconditions.checkNotNull;

public class Utils {

	public static <T> T firstNonNull(@Nullable T a, T b) {
		return a != null ? a : b;
	}

	public static <T> T firstNonNull(@Nullable T a, @Nullable T b, T c) {
		return a != null ? a : (b != null ? b : c);
	}

	@SafeVarargs
	public static <T> T firstNonNull(T... values) {
		for (T value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	public static <T, V> UnaryOperator<T> apply(BiFunction<T, ? super V, T> modifier, V value) {
		return instance -> modifier.apply(instance, value);
	}

	public static <T, V> UnaryOperator<T> applyIf(BiFunction<T, ? super V, T> modifier, V value, Predicate<? super V> predicate) {
		return instance -> {
			if (!predicate.test(value)) return instance;
			return modifier.apply(instance, value);
		};
	}

	public static <T, V> UnaryOperator<T> applyIfNotNull(BiFunction<T, ? super V, T> modifier, V value) {
		return applyIf(modifier, value, Objects::nonNull);
	}

	public static int deepHashCode(@Nullable Object value) {
		if (value == null) return 0;
		if (!value.getClass().isArray()) return value.hashCode();
		if (value instanceof Object[]) return Arrays.deepHashCode((Object[]) value);
		if (value instanceof byte[]) return Arrays.hashCode((byte[]) value);
		if (value instanceof short[]) return Arrays.hashCode((short[]) value);
		if (value instanceof int[]) return Arrays.hashCode((int[]) value);
		if (value instanceof long[]) return Arrays.hashCode((long[]) value);
		if (value instanceof float[]) return Arrays.hashCode((float[]) value);
		if (value instanceof double[]) return Arrays.hashCode((double[]) value);
		if (value instanceof boolean[]) return Arrays.hashCode((boolean[]) value);
		if (value instanceof char[]) return Arrays.hashCode((char[]) value);
		throw new AssertionError();
	}

	public static boolean arraysEquals(
			byte[] array1, int pos1, int len1,
			byte[] array2, int pos2, int len2) {
		if (len1 != len2) return false;
		for (int i = 0; i < len1; i++) {
			if (array1[pos1 + i] != array2[pos2 + i]) {
				return false;
			}
		}
		return true;
	}

	private static byte[] loadResource(InputStream stream) throws IOException {
		// reading file as resource
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = stream.read(buffer)) != -1) {
				out.write(buffer, 0, size);
			}
			return out.toByteArray();
		} finally {
			stream.close();
		}
	}

	public static byte[] loadResource(Path path) throws IOException {
		return loadResource(path.toString());
	}

	public static byte[] loadResource(String name) throws IOException {
		InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
		checkNotNull(resource);
		return loadResource(resource);
	}

	public static InetSocketAddress parseInetSocketAddress(String addressAndPort) throws ParseException {
		int portPos = addressAndPort.lastIndexOf(':');
		if (portPos == -1) {
			try {
				return new InetSocketAddress(Integer.parseInt(addressAndPort));
			} catch (NumberFormatException nfe) {
				throw new ParseException(nfe);
			}
		}
		String addressStr = addressAndPort.substring(0, portPos);
		String portStr = addressAndPort.substring(portPos + 1);
		int port;
		try {
			port = Integer.parseInt(portStr);
		} catch (NumberFormatException nfe) {
			throw new ParseException(nfe);
		}

		if (port <= 0 || port >= 65536) {
			throw new ParseException("Invalid address. Port is not in range (0, 65536) " + addressStr);
		}
		if ("*".equals(addressStr)) {
			return new InetSocketAddress(port);
		}
		try {
			InetAddress address = InetAddress.getByName(addressStr);
			return new InetSocketAddress(address, port);
		} catch (UnknownHostException e) {
			throw new ParseException(e);
		}
	}

	private static final boolean launchedByIntellij = System.getProperty("java.class.path", "").contains("idea_rt.jar");

	@Nullable
	@Contract("_, _ -> null")
	public static <V> V nullify(@Nullable V value, @NotNull Consumer<@NotNull V> action) {
		if (value != null) {
			action.accept(value);
		}
		return null;
	}

	@Nullable
	@Contract("_, _, _ -> null")
	public static <V, T1> V nullify(@Nullable V value, @NotNull BiConsumer<@NotNull V, T1> action, T1 actionArg1) {
		if (value != null) {
			action.accept(value, actionArg1);
		}
		return null;
	}

}
