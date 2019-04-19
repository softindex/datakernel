package io.global.editor.document;

import io.datakernel.ot.OTState;
import io.global.editor.document.name.ChangeDocumentName;

import java.util.List;

import static io.datakernel.util.CollectionUtils.getLast;

public final class DocumentOTState implements OTState<DocumentMultiOperation> {
	private String documentName;
	private StringBuilder content = new StringBuilder();

	@Override
	public void init() {
		documentName = "";
		content.setLength(0);
	}

	@Override
	public void apply(DocumentMultiOperation multiOperation) {
		multiOperation.getEditOps().forEach(op -> op.apply(content));
		List<ChangeDocumentName> documentNameOps = multiOperation.getDocumentNameOps();
		if (!documentNameOps.isEmpty()) {
			documentName = getLast(documentNameOps).getNext();
		}
	}

	public String getDocumentName() {
		return documentName;
	}

	public String getContent() {
		return content.toString();
	}

	public boolean isEmpty() {
		return documentName.equals("") && content.length() == 0;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DocumentOTState that = (DocumentOTState) o;

		if (!documentName.equals(that.documentName)) return false;
		if (content.length() != that.content.length()) return false;
		if (!content.toString().equals(that.content.toString())) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = documentName.hashCode();
		result = 31 * result + content.toString().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "DocumentOTState{" +
				"documentName='" + documentName + '\'' +
				", content=" + content +
				'}';
	}
}
