package io.global.editor.documentlist;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import static java.util.Collections.singletonList;

public final class DocumentListOTSystem {
	private DocumentListOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<DocumentListOperation> createOTSystem() {
		return OTSystemImpl.<DocumentListOperation>create()
				.withTransformFunction(DocumentListOperation.class, DocumentListOperation.class, (left, right) -> TransformResult.of(right, left))
				.withInvertFunction(DocumentListOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(DocumentListOperation.class, DocumentListOperation.class, (op1, op2) -> {
					if (op1.isEmpty()) return op2;
					if (op2.isEmpty()) return op1;
					if (op1.isInversionFor(op2)) {
						return DocumentListOperation.EMPTY;
					}
					return null;
				})
				.withEmptyPredicate(DocumentListOperation.class, DocumentListOperation::isEmpty);
	}

}
