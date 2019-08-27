package io.global.ot.name;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import static io.datakernel.ot.TransformResult.*;
import static io.datakernel.util.Preconditions.checkState;
import static io.global.ot.name.ChangeName.changeName;
import static java.util.Collections.singletonList;

public final class NameOTSystem {
	private NameOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<ChangeName> createOTSystem() {
		return OTSystemImpl.<ChangeName>create()
				.withEmptyPredicate(ChangeName.class, ChangeName::isEmpty)
				.withInvertFunction(ChangeName.class, op -> singletonList(changeName(op.getNext(), op.getPrev(), op.getTimestamp())))

				.withSquashFunction(ChangeName.class, ChangeName.class, (first, second) -> changeName(first.getPrev(), second.getNext(), second.getTimestamp()))

				.withTransformFunction(ChangeName.class, ChangeName.class, (left, right) -> {
					checkState(left.getPrev().equals(right.getPrev()), "Previous values of left and right operations should be equal");
					if (left.getTimestamp() > right.getTimestamp()) {
						return right(changeName(right.getNext(), left.getNext(), left.getTimestamp()));
					}
					if (left.getTimestamp() < right.getTimestamp()) {
						return left(changeName(left.getNext(), right.getNext(), right.getTimestamp()));
					}
					if (left.getNext().compareTo(right.getNext()) > 0) {
						return right(changeName(right.getNext(), left.getNext(), left.getTimestamp()));
					}
					if (left.getNext().compareTo(right.getNext()) < 0) {
						return left(changeName(left.getNext(), right.getNext(), right.getTimestamp()));
					}
					return empty();
				});
	}
}
