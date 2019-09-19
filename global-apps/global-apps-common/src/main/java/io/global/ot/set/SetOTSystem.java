package io.global.ot.set;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import java.util.function.Predicate;

import static java.util.Collections.singletonList;

public final class SetOTSystem {
	private SetOTSystem() {
		throw new AssertionError();
	}

	@SuppressWarnings("unchecked")
	public static <E> OTSystem<SetOperation<E>> createOTSystem(Predicate<E> emptyPredicate) {
		return OTSystemImpl.<SetOperation<E>>create()
				.withEmptyPredicate(SetOperation.class, setOp ->
						setOp == SetOperation.EMPTY || emptyPredicate.test(setOp.getElement()))
				.withInvertFunction(SetOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(SetOperation.class, SetOperation.class, (op1, op2) -> {
					if (emptyPredicate.test(op1.getElement())) {
						return op2;
					}
					if (emptyPredicate.test(op2.getElement())) {
						return op1;
					}
					if (op1.isInversionFor(op2)) {
						return (SetOperation<E>) SetOperation.EMPTY;
					}
					return null;
				})
				.withTransformFunction(SetOperation.class, SetOperation.class, (left, right) -> TransformResult.of(right, left));
	}

}
