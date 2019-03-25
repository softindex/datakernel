package io.global.chat.roomlist;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import static java.util.Collections.singletonList;

public final class RoomListOTSystem {
	private RoomListOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<RoomListOperation> createOTSystem() {
		return OTSystemImpl.<RoomListOperation>create()
				.withTransformFunction(RoomListOperation.class, RoomListOperation.class, (left, right) -> TransformResult.of(right, left))
				.withInvertFunction(RoomListOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(RoomListOperation.class, RoomListOperation.class, (op1, op2) -> {
					if (op1.isEmpty()) return op2;
					if (op2.isEmpty()) return op1;
					if (op1.isInversionFor(op2)) {
						return RoomListOperation.EMPTY;
					}
					return null;
				})
				.withEmptyPredicate(RoomListOperation.class, RoomListOperation::isEmpty);
	}

}
