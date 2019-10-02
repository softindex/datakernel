package io.global.documents;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.documents.document.DocumentMultiOperation;
import io.global.ot.edit.EditOTSystem;
import io.global.ot.edit.EditOperation;
import io.global.ot.value.ChangeValueOTSystem;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.OTUtils.ofChangeValue;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final OTSystem<EditOperation> EDIT_OT_SYSTEM = EditOTSystem.createOTSystem();

	public static final StructuredCodec<DocumentMultiOperation> DOCUMENT_MULTI_OPERATION_CODEC = object(DocumentMultiOperation::new,
			"editOps", DocumentMultiOperation::getEditOps, ofList(EDIT_OPERATION_CODEC),
			"nameOps", DocumentMultiOperation::getDocumentNameOps, ofList(ofChangeValue(STRING_CODEC)));

	public static OTSystem<DocumentMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(DocumentMultiOperation::new,
				DocumentMultiOperation::getEditOps, EditOTSystem.createOTSystem(),
				DocumentMultiOperation::getDocumentNameOps, ChangeValueOTSystem.get());
	}
}
