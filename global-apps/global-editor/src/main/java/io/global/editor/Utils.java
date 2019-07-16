package io.global.editor;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.editor.document.DocumentMultiOperation;
import io.global.editor.document.edit.DeleteOperation;
import io.global.editor.document.edit.EditOTSystem;
import io.global.editor.document.edit.EditOperation;
import io.global.editor.document.edit.InsertOperation;
import io.global.ot.name.NameOTSystem;

import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.global.editor.document.edit.DeleteOperation.DELETE_CODEC;
import static io.global.editor.document.edit.InsertOperation.INSERT_CODEC;
import static io.global.ot.OTUtils.CHANGE_NAME_CODEC;

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

	public static final StructuredCodec<DocumentMultiOperation> DOCUMENT_MULTI_OPERATION_CODEC = object(DocumentMultiOperation::new,
			"editOps", DocumentMultiOperation::getEditOps, ofList(EDIT_OPERATION_CODEC),
			"documentNameOps", DocumentMultiOperation::getDocumentNameOps, ofList(CHANGE_NAME_CODEC));

	public static OTSystem<DocumentMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(DocumentMultiOperation::new,
				DocumentMultiOperation::getEditOps, EditOTSystem.createOTSystem(),
				DocumentMultiOperation::getDocumentNameOps, NameOTSystem.createOTSystem());
	}
}
