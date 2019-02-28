package io.global.ot.editor.operations;

import io.datakernel.codec.StructuredCodec;
import org.jetbrains.annotations.NotNull;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.ot.editor.operations.DeleteOperation.delete;
import static io.global.ot.editor.operations.Utils.limit;

public class InsertOperation implements EditorOperation {
	public static final StructuredCodec<InsertOperation> INSERT_CODEC = object(InsertOperation::new,
			"pos", InsertOperation::getPosition, INT_CODEC,
			"content", InsertOperation::getContent, STRING_CODEC);

	private final int position;
	@NotNull
	private final String content;

	public InsertOperation(int position, @NotNull String content) {
		this.position = position;
		this.content = content;
	}

	public static InsertOperation insert(int position, @NotNull String content) {
		return new InsertOperation(position, content);
	}

	@Override
	public void apply(StringBuilder builder) {
		builder.insert(position, content);
	}

	@Override
	public EditorOperation invert() {
		return delete(position, content);
	}

	@Override
	public int getPosition() {
		return position;
	}

	@NotNull
	@Override
	public String getContent() {
		return content;
	}

	@Override
	public int getLength() {
		return content.length();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		InsertOperation that = (InsertOperation) o;

		if (position != that.position) return false;
		if (!content.equals(that.content)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = position;
		result = 31 * result + content.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Ins@" + position + '[' + limit(content, 10) + ']';
	}
}
