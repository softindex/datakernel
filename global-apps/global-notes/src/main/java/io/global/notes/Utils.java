package io.global.notes;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.global.notes.note.operation.DeleteOperation;
import io.global.notes.note.operation.EditOperation;
import io.global.notes.note.operation.InsertOperation;

import static io.global.notes.note.operation.DeleteOperation.DELETE_CODEC;
import static io.global.notes.note.operation.InsertOperation.INSERT_CODEC;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<EditOperation> EDIT_OPERATION_CODEC = StructuredCodec.ofObject(
			in -> {
				in.readKey("type");
				String type = in.readString();
				in.readKey("value");
				switch (type) {
					case "Insert":
						return INSERT_CODEC.decode(in);
					case "Delete":
						return DELETE_CODEC.decode(in);
					default:
						throw new ParseException("Either Insert or Delete is expected");
				}
			}, (out, item) -> {
				out.writeKey("type");
				if (item instanceof InsertOperation) {
					out.writeString("Insert");
					out.writeKey("value", INSERT_CODEC, (InsertOperation) item);
				} else if (item instanceof DeleteOperation) {
					out.writeString("Delete");
					out.writeKey("value", DELETE_CODEC, (DeleteOperation) item);
				} else {
					throw new IllegalArgumentException("Item should be either InsertOperation or DeleteOperation");
				}
			}
	);

}
