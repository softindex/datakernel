package io.global.editor.document;

import io.global.editor.document.edit.EditOperation;
import io.global.editor.document.name.ChangeDocumentName;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public final class DocumentMultiOperation {
	private final List<EditOperation> editOps;
	private final List<ChangeDocumentName> documentNameOps;

	public DocumentMultiOperation(List<EditOperation> editOps, List<ChangeDocumentName> documentNameOps) {
		this.editOps = editOps;
		this.documentNameOps = documentNameOps;
	}

	public static DocumentMultiOperation create() {
		return new DocumentMultiOperation(new ArrayList<>(), new ArrayList<>());
	}

	public DocumentMultiOperation withEditOps(EditOperation... messageOps) {
		this.editOps.addAll(asList(messageOps));
		return this;
	}

	public DocumentMultiOperation withDocumentNameOps(ChangeDocumentName... roomNameOps) {
		this.documentNameOps.addAll(asList(roomNameOps));
		return this;
	}

	public List<EditOperation> getEditOps() {
		return editOps;
	}

	public List<ChangeDocumentName> getDocumentNameOps() {
		return documentNameOps;
	}
}
