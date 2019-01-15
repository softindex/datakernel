package io.global.ot.chat.operations;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.global.ot.chat.common.Operation;

import static java.util.Collections.singletonList;

public class ChatOTSystem {

	public static OTSystem<Operation> createOTSystem() {
		return OTSystemImpl.<Operation>create()
				.withEmptyPredicate(ChatOperation.class, operation -> operation.getContent().isEmpty())
				.withInvertFunction(ChatOperation.class, op -> singletonList(op.invert()))
				.withTransformFunction(ChatOperation.class, ChatOperation.class, (left, right) -> TransformResult.of(right, left));
	}
}
