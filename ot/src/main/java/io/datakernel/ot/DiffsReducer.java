package io.datakernel.ot;

import io.datakernel.annotation.Nullable;
import io.datakernel.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public interface DiffsReducer<A, D> {
	A initialValue();

	A accumulate(A accumulatedDiffs, List<D> diffs);

	A combine(A existing, A additional);

	static <A, D> DiffsReducer<A, D> of(@Nullable A initialValue, BiFunction<A, List<D>, A> reduceFunction) {
		return of(initialValue, reduceFunction, (existing, additional) -> existing);
	}

	static <A, D> DiffsReducer<A, D> of(@Nullable A initialValue, BiFunction<A, List<D>, A> reduceFunction, BinaryOperator<A> combiner) {
		return new DiffsReducer<A, D>() {
			@Nullable
			@Override
			public A initialValue() {
				return initialValue;
			}

			@Override
			public A accumulate(A accumulatedDiffs, List<D> diffs) {
				return reduceFunction.apply(accumulatedDiffs, diffs);
			}

			@Override
			public A combine(A existing, A additional) {
				return combiner.apply(existing, additional);
			}
		};
	}

	static <D> DiffsReducer<Void, D> toVoid() {
		return of(null, ($, lists) -> null);
	}

	static <D> DiffsReducer<List<D>, D> toList() {
		return of(new ArrayList<>(), (accumulatedDiffs, diffs) ->
				CollectionUtils.concat(diffs, accumulatedDiffs));
	}

	static <D> DiffsReducer<List<D>, D> toSquashedList(OTSystem<D> system) {
		return of(new ArrayList<>(), (accumulatedDiffs, diffs) ->
				system.squash(CollectionUtils.concat(diffs, accumulatedDiffs)));
	}

}
