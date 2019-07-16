package io.global.editor.document.edit;

public interface EditOperation {
	void apply(StringBuilder builder);

	EditOperation invert();

	int getPosition();

	String getContent();

	int getLength();

}
