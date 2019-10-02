package io.global.documents.document;

import io.global.ot.edit.EditOperation;
import io.global.ot.value.ChangeValue;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public final class DocumentMultiOperation {
	private final List<EditOperation> editOps;
	private final List<ChangeValue<String>> documentNameOps;

	public DocumentMultiOperation(List<EditOperation> editOps, List<ChangeValue<String>> documentNameOps) {
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

	@SafeVarargs
	public final DocumentMultiOperation withDocumentNameOps(ChangeValue<String>... roomNameOps) {
		this.documentNameOps.addAll(asList(roomNameOps));
		return this;
	}

	public List<EditOperation> getEditOps() {
		return editOps;
	}

	public List<ChangeValue<String>> getDocumentNameOps() {
		return documentNameOps;
	}
}
