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

package io.datakernel.util;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class Preconditions {
	private Preconditions() {
	}

	public static void check(boolean expression) {
		if (!expression) {
			throw new RuntimeException();
		}
	}

	public static void check(boolean expression, Object message) {
		if (!expression) {
			throw new RuntimeException(String.valueOf(message));
		}
	}

	public static void check(boolean expression, Supplier<String> message) {
		if (!expression) {
			throw new RuntimeException(message.get());
		}
	}

	public static void check(boolean expression, String template, Object... args) {
		if (!expression) {
			throw new RuntimeException(String.format(template, args));
		}
	}

	public static <T> T checkNotNull(T reference) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException();
	}

	public static <T> T checkNotNull(T reference, Object message) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException(String.valueOf(message));
	}

	public static <T> T checkNotNull(T reference, Supplier<String> message) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException(message.get());
	}

	public static <T> T checkNotNull(T reference, String template, Object... args) {
		if (reference != null) {
			return reference;
		}
		throw new NullPointerException(String.format(template, args));
	}

	public static void checkState(boolean expression) {
		if (!expression) {
			throw new IllegalStateException();
		}
	}

	public static void checkState(boolean expression, Object message) {
		if (!expression) {
			throw new IllegalStateException(String.valueOf(message));
		}
	}

	public static void checkState(boolean expression, Supplier<String> message) {
		if (!expression) {
			throw new IllegalStateException(message.get());
		}
	}

	public static void checkState(boolean expression, String template, Object... args) {
		if (!expression) {
			throw new IllegalStateException(String.format(template, args));
		}
	}

	public static void checkArgument(boolean expression) {
		if (!expression) {
			throw new IllegalArgumentException();
		}
	}

	public static void checkArgument(boolean expression, Object message) {
		if (!expression) {
			throw new IllegalArgumentException(String.valueOf(message));
		}
	}

	public static void checkArgument(boolean expression, Supplier<String> message) {
		if (!expression) {
			throw new IllegalArgumentException(message.get());
		}
	}

	public static void checkArgument(boolean expression, String template, Object... args) {
		if (!expression) {
			throw new IllegalArgumentException(String.format(template, args));
		}
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException();
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, Object message) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(String.valueOf(message));
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, Supplier<String> message) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(message.get());
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, Function<T, String> message) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(message.apply(argument));
	}

	public static <T> T checkArgument(T argument, Predicate<T> predicate, String template, Object... args) {
		if (predicate.test(argument)) {
			return argument;
		}
		throw new IllegalArgumentException(String.format(template, args));
	}
}
