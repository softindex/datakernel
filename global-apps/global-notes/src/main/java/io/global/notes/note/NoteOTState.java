package io.global.notes.note;

import io.datakernel.ot.OTState;
import io.global.notes.note.operation.EditOperation;

public final class NoteOTState implements OTState<EditOperation> {
	private StringBuilder content = new StringBuilder();

	@Override
	public void init() {
		content.setLength(0);
	}

	@Override
	public void apply(EditOperation editOperation) {
		editOperation.apply(content);
	}

	public String getContent() {
		return content.toString();
	}

	public boolean isEmpty() {
		return content.length() == 0;
	}

	@Override
	public String toString() {
		int length = content.length();
		return "DocumentOTState{'" +
				(length > 100 ?
						content.substring(0, 100) + "...' + " + (length - 100) + " more symbols" :
						content.toString() + '\'') +
				'}';
	}
}
