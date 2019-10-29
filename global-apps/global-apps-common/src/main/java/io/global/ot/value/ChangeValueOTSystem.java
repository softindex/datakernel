package io.global.ot.value;

import io.datakernel.common.Preconditions;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import static io.datakernel.ot.TransformResult.*;
import static java.util.Collections.singletonList;

public final class ChangeValueOTSystem {
	private static final OTSystemImpl<ChangeValue<?>> SYSTEM = OTSystemImpl.<ChangeValue<?>>create()
			.withEmptyPredicate(ChangeValue.class, ChangeValue::isEmpty)
			.withInvertFunction(ChangeValue.class, op -> singletonList(ChangeValue.of(op.getNext(), op.getPrev(), op.getTimestamp())))

			.withSquashFunction(ChangeValue.class, ChangeValue.class, (first, second) -> ChangeValue.of(first.getPrev(), second.getNext(), second.getTimestamp()))

			.withTransformFunction(ChangeValue.class, ChangeValue.class, (left, right) -> {
				Preconditions.checkState(left.getPrev().equals(right.getPrev()), "Previous values of left and right operations should be equal");

				if (left.getTimestamp() > right.getTimestamp()) {
					return right(ChangeValue.of(right.getNext(), left.getNext(), left.getTimestamp()));
				}
				if (left.getTimestamp() < right.getTimestamp()) {
					return left(ChangeValue.of(left.getNext(), right.getNext(), right.getTimestamp()));
				}
				if (!left.getNext().equals(right.getNext())) {
					return right(ChangeValue.of(right.getNext(), left.getNext(), left.getTimestamp()));
				}
				return empty();
			});

	private ChangeValueOTSystem() {
		throw new AssertionError();
	}

	@SuppressWarnings("unchecked")
	public static <T> OTSystem<ChangeValue<T>> get() {
		return (OTSystem<ChangeValue<T>>) (OTSystem) SYSTEM;
	}
}
