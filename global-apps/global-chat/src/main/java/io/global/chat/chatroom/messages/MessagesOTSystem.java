package io.global.chat.chatroom.messages;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import static java.util.Collections.singletonList;

public final class MessagesOTSystem {
	private MessagesOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<MessageOperation> createOTSystem() {
		return OTSystemImpl.<MessageOperation>create()
				.withEmptyPredicate(MessageOperation.class, MessageOperation::isEmpty)
				.withInvertFunction(MessageOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(MessageOperation.class, MessageOperation.class, (op1, op2) -> {
					if (op1.isEmpty()) {
						return op2;
					}
					if (op2.isEmpty()) {
						return op1;
					}
					if (op1.isInversionFor(op2)) {
						return MessageOperation.EMPTY;
					}
					return null;
				})
				.withTransformFunction(MessageOperation.class, MessageOperation.class, (left, right) -> TransformResult.of(right, left));
	}
}
