package io.global.chat.friendlist;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import static java.util.Collections.singletonList;

public final class FriendListOTSystem {
	private FriendListOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<FriendListOperation> createOTSystem() {
		return OTSystemImpl.<FriendListOperation>create()
				.withTransformFunction(FriendListOperation.class, FriendListOperation.class, ((left, right) -> TransformResult.of(right, left)))
				.withInvertFunction(FriendListOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(FriendListOperation.class, FriendListOperation.class, (op1, op2) -> {
					if (op1.isEmpty()) return op2;
					if (op2.isEmpty()) return op1;
					if (op1.isInversionFor(op2)) {
						return FriendListOperation.EMPTY;
					}
					return null;
				})
				.withEmptyPredicate(FriendListOperation.class, FriendListOperation::isEmpty);
	}
}
