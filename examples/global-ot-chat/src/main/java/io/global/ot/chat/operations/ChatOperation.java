package io.global.ot.chat.operations;

import io.datakernel.codec.StructuredCodec;

import static io.datakernel.codec.StructuredCodecs.*;

public final class ChatOperation {
	public static final ChatOperation EMPTY = new ChatOperation(0, "", "", false);
	public static final StructuredCodec<ChatOperation> OPERATION_CODEC = object(ChatOperation::new,
			"timestamp", ChatOperation::getTimestamp, LONG_CODEC,
			"author", ChatOperation::getAuthor, STRING_CODEC,
			"content", ChatOperation::getContent, STRING_CODEC,
			"isDelete", ChatOperation::isTombstone, BOOLEAN_CODEC);

	private final long timestamp;
	private final String author;
	private final String content;
	private final boolean isTombstone;

	private ChatOperation(long timestamp, String author, String content, boolean remove) {
		this.timestamp = timestamp;
		this.author = author;
		this.content = content;
		this.isTombstone = remove;
	}

	public static ChatOperation insert(long timestamp, String author, String content) {
		return new ChatOperation(timestamp, author, content, false);
	}

	public static ChatOperation delete(long timestamp, String author, String content) {
		return new ChatOperation(timestamp, author, content, true);
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getContent() {
		return content;
	}

	public String getAuthor() {
		return author;
	}

	public boolean isTombstone() {
		return isTombstone;
	}

	public boolean isEmpty() {
		return content.isEmpty() || author.isEmpty();
	}

	public ChatOperation invert() {
		return new ChatOperation(timestamp, author, content, !isTombstone);
	}

	public boolean isInversionFor(ChatOperation operation) {
		return timestamp == operation.timestamp &&
				author.equals(operation.author) &&
				content.equals(operation.content) &&
				isTombstone != operation.isTombstone;
	}

	@Override
	public String toString() {
		return '{' + (isTombstone ? "-" : "+") +
				content +
				" [" + author +
				"]}";
	}
}
