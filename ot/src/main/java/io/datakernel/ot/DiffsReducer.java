package io.datakernel.ot;

import io.datakernel.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public interface DiffsReducer<A, D> {
	A initialValue();

	A accumulate(A accumulatedDiffs, List<D> diffs);

	static <A, D> DiffsReducer<A, D> of(A initialValue, BiFunction<A, List<D>, A> reduceFunction) {
		return new DiffsReducer<A, D>() {
			@Override
			public A initialValue() {
				return initialValue;
			}

			@Override
			public A accumulate(A accumulatedDiffs, List<D> diffs) {
				return reduceFunction.apply(accumulatedDiffs, diffs);
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
