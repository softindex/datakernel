package io.global.editor.document.name;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;

import static io.datakernel.ot.TransformResult.*;
import static io.datakernel.util.Preconditions.checkState;
import static io.global.editor.document.name.ChangeDocumentName.changeName;
import static java.util.Collections.singletonList;

public final class DocumentNameOTSystem {
	private DocumentNameOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<ChangeDocumentName> createOTSystem() {
		return OTSystemImpl.<ChangeDocumentName>create()
				.withEmptyPredicate(ChangeDocumentName.class, ChangeDocumentName::isEmpty)
				.withInvertFunction(ChangeDocumentName.class, op -> singletonList(changeName(op.getNext(), op.getPrev(), op.getTimestamp())))

				.withSquashFunction(ChangeDocumentName.class, ChangeDocumentName.class, (first, second) -> changeName(first.getPrev(), second.getNext(), second.getTimestamp()))
				.withTransformFunction(ChangeDocumentName.class, ChangeDocumentName.class, (left, right) -> {
					checkState(left.getPrev().equals(right.getPrev()), "Previous values of left and right operation should be equal");
					if (left.getTimestamp() > right.getTimestamp())
						return right(changeName(right.getNext(), left.getNext(), left.getTimestamp()));
					if (left.getTimestamp() < right.getTimestamp())
						return left(changeName(left.getNext(), right.getNext(), right.getTimestamp()));
					if (left.getNext().compareTo(right.getNext()) > 0)
						return right(changeName(right.getNext(), left.getNext(), left.getTimestamp()));
					if (left.getNext().compareTo(right.getNext()) < 0)
						return left(changeName(left.getNext(), right.getNext(), right.getTimestamp()));
					return empty();
				});
	}
}
