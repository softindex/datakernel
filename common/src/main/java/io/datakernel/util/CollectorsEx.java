package io.datakernel.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Collector;

public class CollectorsEx {

	private CollectorsEx() {}

	public static final Collector<Object, Void, Void> TO_VOID = Collector.of(() -> null, (a, v) -> {}, (a1, a2) -> null, a -> null);

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, Void, Void> toVoid() {
		return (Collector<T, Void, Void>) TO_VOID;
	}

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, T[], T> toFirst() {
		return Collector.of(
				() -> (T[]) new Object[1],
				(a, v) -> { if (a[0] == null) a[0] = v; },
				(a1, a2) -> a1,
				a -> a[0]);
	}

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, T[], T> toLast() {
		return Collector.of(
				() -> (T[]) new Object[1],
				(a, v) -> a[0] = v,
				(a1, a2) -> a1,
				a -> a[0]);
	}

	@SuppressWarnings("unchecked")
	public static <T> Collector<T, List<T>, T[]> toArray(Class<T> type) {
		return Collector.of(
				ArrayList::new,
				List::add,
				(left, right) -> { left.addAll(right); return left; },
				a -> a.toArray((T[]) Array.newInstance(type, a.size())));
	}

	private static final Collector<Boolean, Boolean, Boolean> TO_ALL =
			Collector.of(() -> true, (a, t) -> a &= t, (a1, a2) -> a1 & a2);

	private static final Collector<Boolean, Boolean, Boolean> TO_ANY =
			Collector.of(() -> true, (a, t) -> a |= t, (a1, a2) -> a1 || a2);

	private static final Collector<Boolean, Boolean, Boolean> TO_NONE =
			Collector.of(() -> true, (a, t) -> a &= !t, (a1, a2) -> a1 && a2);

	public static <T> Collector<T, Boolean, Boolean> toAll(Predicate<? super T> predicate) {
		return Collector.of(() -> true, (a, t) -> a &= predicate.test(t), (a1, a2) -> a1 && a2);
	}

	public static Collector<Boolean, Boolean, Boolean> toAll() {
		return TO_ALL;
	}

	public static <T> Collector<T, Boolean, Boolean> toAny(Predicate<T> predicate) {
		return Collector.of(() -> false, (a, t) -> a |= predicate.test(t), (a1, a2) -> a1 || a2);
	}

	public static Collector<Boolean, Boolean, Boolean> toAny() {
		return TO_ANY;
	}

	public static <T> Collector<T, Boolean, Boolean> toNone(Predicate<T> predicate) {
		return Collector.of(() -> true, (a, t) -> a &= !predicate.test(t), (a1, a2) -> a1 && a2);
	}

	public static Collector<Boolean, Boolean, Boolean> toNone() {
		return TO_NONE;
	}

	public static <T> BinaryOperator<T> throwingMerger() {
		return (u, v) -> { throw new IllegalStateException("Duplicate key " + u); };
	}

}
