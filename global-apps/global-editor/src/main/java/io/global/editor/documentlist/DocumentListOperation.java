package io.global.editor.documentlist;

import static java.util.Collections.emptySet;

public class DocumentListOperation {
	public static final DocumentListOperation EMPTY = new DocumentListOperation(new Document("", emptySet()), true);

	private final Document document;
	private final boolean remove;

	public DocumentListOperation(Document document, boolean remove) {
		this.document = document;
		this.remove = remove;
	}

	public static DocumentListOperation create(Document document) {
		return new DocumentListOperation(document, false);
	}

	public static DocumentListOperation delete(Document document) {
		return new DocumentListOperation(document, true);
	}

	public DocumentListOperation invert() {
		return new DocumentListOperation(document, !remove);
	}

	public boolean isEmpty() {
		return document.getParticipants().isEmpty();
	}

	public boolean isRemove() {
		return remove;
	}

	public Document getDocument() {
		return document;
	}

	public boolean isInversionFor(DocumentListOperation other) {
		return document.equals(other.document) && remove != other.remove;
	}
}
