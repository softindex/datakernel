package io.global.ot.shared;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import static java.util.Collections.singletonList;

public final class SharedReposOTSystem {
	private SharedReposOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<SharedReposOperation> createOTSystem() {
		return OTSystemImpl.<SharedReposOperation>create()
				.withTransformFunction(SharedReposOperation.class, SharedReposOperation.class, (left, right) -> TransformResult.of(right, left))
				.withInvertFunction(SharedReposOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(SharedReposOperation.class, SharedReposOperation.class, (op1, op2) -> {
					if (op1.isEmpty()) return op2;
					if (op2.isEmpty()) return op1;
					if (op1.isInversionFor(op2)) {
						return SharedReposOperation.EMPTY;
					}
					return null;
				})
				.withEmptyPredicate(SharedReposOperation.class, SharedReposOperation::isEmpty);
	}

}
