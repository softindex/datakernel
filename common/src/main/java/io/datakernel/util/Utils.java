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

import io.datakernel.annotation.Nullable;
import io.datakernel.exception.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.*;

import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings("UnnecessaryLocalVariable")
public class Utils {
	private Utils() {
	}

	@Nullable
	@SafeVarargs
	public static <T> T coalesce(T... values) {
		for (T value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	public static <T> T coalesce(@Nullable T a, T b) {
		return a != null ? a : b;
	}

	public static <T> T coalesce(@Nullable T a, @Nullable T b, T c) {
		return a != null ? a : (b != null ? b : c);
	}

	@Nullable
	public static <T, R> R apply(@Nullable T value, Function<? super T, ? extends R> fn) {
		return value == null ? null : fn.apply(value);
	}

	public static <T> boolean test(@Nullable T value, Predicate<? super T> predicate) {
		return value != null && predicate.test(value);
	}

	@Nullable
	public static <T> T accept(@Nullable T value, Consumer<? super T> consumer) {
		if (value == null) return null;
		consumer.accept(value);
		return value;
	}

	@Nullable
	public static <T, R> R transform(@Nullable T seed, Function<T, R> fn) {
		if (seed == null) return null;
		R r = fn.apply(seed);
		return r;
	}

	@Nullable
	public static <T, R1, R2> R2 transform(@Nullable T seed, Function<T, R1> fn1, Function<R1, R2> fn2) {
		if (seed == null) return null;
		R1 r1 = fn1.apply(seed);
		if (r1 == null) return null;
		R2 r2 = fn2.apply(r1);
		return r2;
	}

	@Nullable
	public static <T, R1, R2, R3> R3 transform(@Nullable T seed, Function<T, R1> fn1, Function<R1, R2> fn2, Function<R2, R3> fn3) {
		if (seed == null) return null;
		R1 r1 = fn1.apply(seed);
		if (r1 == null) return null;
		R2 r2 = fn2.apply(r1);
		if (r2 == null) return null;
		R3 r3 = fn3.apply(r2);
		return r3;
	}

	@Nullable
	public static <T, R1, R2, R3, R4> R4 transform(@Nullable T seed, Function<T, R1> fn1, Function<R1, R2> fn2, Function<R2, R3> fn3, Function<R3, R4> fn4) {
		if (seed == null) return null;
		R1 r1 = fn1.apply(seed);
		if (r1 == null) return null;
		R2 r2 = fn2.apply(r1);
		if (r2 == null) return null;
		R3 r3 = fn3.apply(r2);
		if (r3 == null) return null;
		R4 r4 = fn4.apply(r3);
		return r4;
	}

	@Nullable
	public static <T> T transform(@Nullable T seed, List<? extends Function<? super T, ? extends T>> fns) {
		for (Function<? super T, ? extends T> fn : fns) {
			seed = fn.apply(seed);
		}
		return seed;
	}

	public static String nullToEmpty(@Nullable String string) {
		return string != null ? string : "";
	}

	public static <V> void set(Consumer<? super V> op, V value) {
		op.accept(value);
	}

	public static <V> void setIf(Consumer<? super V> op, V value, Predicate<? super V> predicate) {
		if (!predicate.test(value)) return;
		op.accept(value);
	}

	public static <V> void setIfNotNull(Consumer<? super V> op, V value) {
		setIf(op, value, Objects::nonNull);
	}

	public static <T, V> void set(Function<? super V, T> setter, V value) {
		setter.apply(value);
	}

	public static <T, V> void setIf(Function<? super V, T> setter, V value, Predicate<? super V> predicate) {
		if (!predicate.test(value)) return;
		setter.apply(value);
	}

	public static <T, V> void setIfNotNull(Function<? super V, T> setter, V value) {
		setIf(setter, value, Objects::nonNull);
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

	public static Consumer<Boolean> ifTrue(Runnable runnable) {
		return b -> {
			if (b) {
				runnable.run();
			}
		};
	}

	public static Consumer<Boolean> ifFalse(Runnable runnable) {
		return b -> {
			if (!b) {
				runnable.run();
			}
		};
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

	public static byte[] loadResource(File file) throws IOException {
		return loadResource(file.getPath());
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

	/**
	 * Debug method which outputs messages with file name and line where it was called,
	 * formatted in a way so that IntelliJ would pick it up and make links in the console.
	 *
	 * @param message any data to be printed.
	 * @throws AssertionError when called from an application that was not
	 *                        launched by the IntelliJ Idea IDE
	 */
	@SuppressWarnings("UseOfSystemOutOrSystemErr")
	public static void DEBUG(Object message) {
		if (!launchedByIntellij) {
			throw new AssertionError("Debug message call when not launched in an IDE!");
		}
		StackTraceElement caller = Thread.currentThread().getStackTrace()[2];
		System.out.printf("DEBUG.(%s:%d).%s| %s%n", caller.getFileName(), caller.getLineNumber(), caller.getMethodName(), message);
	}

}
