package io.datakernel.util;

import io.datakernel.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.*;

@SuppressWarnings("UnnecessaryLocalVariable")
public class Utils {
	private Utils() {
	}

	@SafeVarargs
	public static <T> T coalesce(T... values) {
		for (T value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	public static <T> T coalesce(T a, T b) {
		return a != null ? a : b;
	}

	public static <T> T coalesce(T a, T b, T c) {
		return a != null ? a : (b != null ? b : c);
	}

	public static <T, R> R transform(@Nullable T seed, Function<T, R> fn) {
		if (seed == null) return null;
		R r = fn.apply(seed);
		return r;
	}

	public static <T, R1, R2> R2 transform(@Nullable T seed, Function<T, R1> fn1, Function<R1, R2> fn2) {
		if (seed == null) return null;
		R1 r1 = fn1.apply(seed);
		if (r1 == null) return null;
		R2 r2 = fn2.apply(r1);
		return r2;
	}

	public static <T, R1, R2, R3> R3 transform(@Nullable T seed, Function<T, R1> fn1, Function<R1, R2> fn2, Function<R2, R3> fn3) {
		if (seed == null) return null;
		R1 r1 = fn1.apply(seed);
		if (r1 == null) return null;
		R2 r2 = fn2.apply(r1);
		if (r2 == null) return null;
		R3 r3 = fn3.apply(r2);
		return r3;
	}

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

	public static <T> T transform(@Nullable T seed, List<? extends Function<? super T, ? extends T>> fns) {
		for (Function<? super T, ? extends T> fn : fns) {
			seed = fn.apply(seed);
		}
		return seed;
	}

	public static String nullToEmpty(@Nullable String string) {
		return string != null ? string : "";
	}

	public static <T, V> void set(Consumer<? super V> op, V value) {
		op.accept(value);
	}

	public static <T, V> void setIf(Consumer<? super V> op, V value, Predicate<? super V> predicate) {
		if (!predicate.test(value)) return;
		op.accept(value);
	}

	public static <T, V> void setIfNotNull(Consumer<? super V> op, V value) {
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

}
