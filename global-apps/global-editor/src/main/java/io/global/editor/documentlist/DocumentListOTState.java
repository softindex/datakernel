package io.global.editor.documentlist;

import io.datakernel.ot.OTState;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public final class DocumentListOTState implements OTState<DocumentListOperation> {
	private static final Consumer<DocumentListOperation> NO_ACTION = op -> {};

	private final Set<Document> documents = new HashSet<>();
	private Consumer<DocumentListOperation> listener = NO_ACTION;

	@Override
	public void init() {
		documents.clear();
	}

	@Override
	public void apply(DocumentListOperation op) {
		if (op.isEmpty()) return;

		if (op.isRemove()) {
			documents.remove(op.getDocument());
		} else {
			documents.add(op.getDocument());
		}
		listener.accept(op);
	}

	public Set<Document> getDocuments() {
		return documents;
	}

	public void setListener(Consumer<DocumentListOperation> listener) {
		this.listener = listener;
	}
}
