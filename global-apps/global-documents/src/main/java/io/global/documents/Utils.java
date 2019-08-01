package io.global.documents;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.documents.document.DocumentMultiOperation;
import io.global.documents.document.edit.DeleteOperation;
import io.global.documents.document.edit.EditOTSystem;
import io.global.documents.document.edit.EditOperation;
import io.global.documents.document.edit.InsertOperation;
import io.global.ot.name.NameOTSystem;

import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.global.documents.document.edit.DeleteOperation.DELETE_CODEC;
import static io.global.documents.document.edit.InsertOperation.INSERT_CODEC;
import static io.global.ot.OTUtils.CHANGE_NAME_CODEC;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<EditOperation> EDIT_OPERATION_CODEC = CodecSubtype.<EditOperation>create()
			.with(InsertOperation.class, "Insert", INSERT_CODEC)
			.with(DeleteOperation.class, "Delete", DELETE_CODEC)
			.withTagName("type", "value");

	public static final StructuredCodec<DocumentMultiOperation> DOCUMENT_MULTI_OPERATION_CODEC = object(DocumentMultiOperation::new,
			"editOps", DocumentMultiOperation::getEditOps, ofList(EDIT_OPERATION_CODEC),
			"nameOps", DocumentMultiOperation::getDocumentNameOps, ofList(CHANGE_NAME_CODEC));

	public static OTSystem<DocumentMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(DocumentMultiOperation::new,
				DocumentMultiOperation::getEditOps, EditOTSystem.createOTSystem(),
				DocumentMultiOperation::getDocumentNameOps, NameOTSystem.createOTSystem());
	}
}
