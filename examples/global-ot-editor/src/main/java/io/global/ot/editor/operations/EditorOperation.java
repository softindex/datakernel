package io.global.ot.editor.operations;

public interface EditorOperation {
	void apply(StringBuilder builder);

	EditorOperation invert();

	int getPosition();

	String getContent();

	int getLength();

}
