package io.global.ot.edit;

public interface EditOperation {
	void apply(StringBuilder builder);

	EditOperation invert();

	int getPosition();

	String getContent();

	int getLength();

}
