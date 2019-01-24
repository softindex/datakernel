package io.global.ot.chat.operations;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.ot.chat.operations.ChatOTState.ChatEntry;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;

public final class ChatOperation {
	public static final ChatOperation EMPTY = new ChatOperation(new ChatEntry(0, "", ""), false);
	public static final StructuredCodec<ChatOperation> OPERATION_CODEC = StructuredCodecs.tuple(
			(a, b, c, d) -> new ChatOperation(new ChatEntry(a, b, c), d),
			ChatOperation::getTimestamp, LONG_CODEC,
			ChatOperation::getAuthor, STRING_CODEC,
			ChatOperation::getContent, STRING_CODEC,
			ChatOperation::isTombstone, BOOLEAN_CODEC);

	private final ChatEntry entry;
	private final boolean isTombstone;

	private ChatOperation(ChatEntry entry, boolean remove) {
		this.entry = entry;
		this.isTombstone = remove;
	}

	public static ChatOperation insert(long timestamp, String author, String content) {
		return new ChatOperation(new ChatEntry(timestamp, author, content), false);
	}

	public static ChatOperation delete(long timestamp, String author, String content) {
		return new ChatOperation(new ChatEntry(timestamp, author, content), true);
	}

	public void apply(Set<ChatEntry> entries) {
		if (isTombstone) {
			entries.remove(entry);
		} else {
			entries.add(entry);
		}
	}

	public long getTimestamp() {
		return entry.getTimestamp();
	}

	public String getContent() {
		return entry.getContent();
	}

	public String getAuthor() {
		return entry.getAuthor();
	}

	public boolean isTombstone() {
		return isTombstone;
	}

	public boolean isEmpty() {
		return getContent().isEmpty() || getAuthor().isEmpty();
	}

	public ChatOperation invert() {
		return new ChatOperation(entry, !isTombstone);
	}

	public boolean isInversionFor(ChatOperation operation) {
		return entry.equals(operation.entry) && isTombstone != operation.isTombstone;
	}

	@Override
	public String toString() {
		return '{' + (isTombstone ? "-" : "+") +
				entry.getContent() +
				" [" + entry.getAuthor() +
				"]}";
	}
}
