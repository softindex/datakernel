package io.global.notes.note.operation;

public interface EditOperation {
	void apply(StringBuilder builder);

	EditOperation invert();

	int getPosition();

	String getContent();

	int getLength();

}
