package io.global.editor;

import io.datakernel.async.RetryPolicy;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.common.PubKey;
import io.global.editor.document.DocumentMultiOperation;
import io.global.editor.document.edit.DeleteOperation;
import io.global.editor.document.edit.EditOTSystem;
import io.global.editor.document.edit.EditOperation;
import io.global.editor.document.edit.InsertOperation;
import io.global.editor.document.name.ChangeDocumentName;
import io.global.editor.document.name.DocumentNameOTSystem;
import io.global.editor.documentlist.Document;
import io.global.editor.documentlist.DocumentListOperation;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.editor.document.edit.DeleteOperation.DELETE_CODEC;
import static io.global.editor.document.edit.InsertOperation.INSERT_CODEC;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final RetryPolicy POLL_RETRY_POLICY = RetryPolicy.exponentialBackoff(1000, 1000 * 60);

	public static final StructuredCodec<PubKey> PUB_KEY_HEX_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);

	public static final StructuredCodec<Document> DOCUMENT_CODEC = object(Document::new,
			"id", Document::getId, STRING_CODEC,
			"participants", Document::getParticipants, ofSet(PUB_KEY_HEX_CODEC));

	public static final StructuredCodec<DocumentListOperation> DOCUMENT_LIST_OPERATION_CODEC = object(DocumentListOperation::new,
			"room", DocumentListOperation::getDocument, DOCUMENT_CODEC,
			"remove", DocumentListOperation::isRemove, BOOLEAN_CODEC);

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

	public static final StructuredCodec<ChangeDocumentName> CHANGE_DOCUMENT_NAME_CODEC = object(ChangeDocumentName::new,
			"prev", ChangeDocumentName::getPrev, STRING_CODEC,
			"next", ChangeDocumentName::getNext, STRING_CODEC,
			"timestamp", ChangeDocumentName::getTimestamp, LONG_CODEC);

	public static final StructuredCodec<DocumentMultiOperation> DOCUMENT_MULTI_OPERATION_CODEC = object(DocumentMultiOperation::new,
			"messageOps", DocumentMultiOperation::getEditOps, ofList(EDIT_OPERATION_CODEC),
			"roomNameOps", DocumentMultiOperation::getDocumentNameOps, ofList(CHANGE_DOCUMENT_NAME_CODEC));

	public static OTSystem<DocumentMultiOperation> createMergedOTSystem() {
		return MergedOTSystem.mergeOtSystems(DocumentMultiOperation::new,
				DocumentMultiOperation::getEditOps, EditOTSystem.createOTSystem(),
				DocumentMultiOperation::getDocumentNameOps, DocumentNameOTSystem.createOTSystem());
	}
}
