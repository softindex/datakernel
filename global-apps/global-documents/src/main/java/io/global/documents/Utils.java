package io.global.documents;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.documents.document.DocumentMultiOperation;
import io.global.ot.edit.EditOTSystem;
import io.global.ot.edit.EditOperation;
import io.global.ot.name.NameOTSystem;

import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.global.ot.OTUtils.CHANGE_NAME_CODEC;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final OTSystem<EditOperation> EDIT_OT_SYSTEM = EditOTSystem.createOTSystem();

	public static final StructuredCodec<DocumentMultiOperation> DOCUMENT_MULTI_OPERATION_CODEC = object(DocumentMultiOperation::new,
			"editOps", DocumentMultiOperation::getEditOps, ofList(EDIT_OPERATION_CODEC),
			"nameOps", DocumentMultiOperation::getDocumentNameOps, ofList(CHANGE_NAME_CODEC));

	public static OTSystem<DocumentMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(DocumentMultiOperation::new,
				DocumentMultiOperation::getEditOps, EditOTSystem.createOTSystem(),
				DocumentMultiOperation::getDocumentNameOps, NameOTSystem.createOTSystem());
	}
}
