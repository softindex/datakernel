package io.global.ot.editor.operations;

import io.datakernel.codec.StructuredCodec;
import org.jetbrains.annotations.NotNull;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.ot.editor.operations.InsertOperation.insert;
import static io.global.ot.editor.operations.Utils.limit;

public class DeleteOperation implements EditorOperation {
	public static final StructuredCodec<DeleteOperation> DELETE_CODEC = object(DeleteOperation::new,
			"pos", DeleteOperation::getPosition, INT_CODEC,
			"content", DeleteOperation::getContent, STRING_CODEC);

	@NotNull
	private final String content;
	private final int position;

	public DeleteOperation(int position, @NotNull String content) {
		this.position = position;
		this.content = content;
	}

	public static DeleteOperation delete(int position, @NotNull String content) {
		return new DeleteOperation(position, content);
	}

	@Override
	public void apply(StringBuilder builder) {
		builder.delete(position, position + content.length());
	}

	@Override
	public EditorOperation invert() {
		return insert(position, content);
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

		DeleteOperation that = (DeleteOperation) o;

		if (position != that.position) return false;
		if (!content.equals(that.content)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = content.hashCode();
		result = 31 * result + position;
		return result;
	}

	@Override
	public String toString() {
		return "Del@" + position + '[' + limit(content, 10) + ']';
	}
}
