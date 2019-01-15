package io.global.ot.chat.operations;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.global.ot.chat.common.Operation;
import io.global.ot.chat.operations.ChatOTState.ChatEntry;

import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;

public class ChatOperation implements Operation {
	public static final StructuredCodec<ChatOperation> OPERATION_CODEC = StructuredCodecs.tuple(
			(a, b, c) -> new ChatOperation(new ChatEntry(a, b), c),
			ChatOperation::getTimestamp, LONG_CODEC,
			ChatOperation::getContent, STRING_CODEC,
			ChatOperation::isTombstone, BOOLEAN_CODEC);

	private final ChatEntry entry;
	private final boolean isTombstone;

	private ChatOperation(ChatEntry entry, boolean remove) {
		this.entry = entry;
		this.isTombstone = remove;
	}

	public static ChatOperation insert(long timestamp, String content) {
		return new ChatOperation(new ChatEntry(timestamp, content), false);
	}

	public static ChatOperation delete(long timestamp, String content) {
		return new ChatOperation(new ChatEntry(timestamp, content), true);
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

	public boolean isTombstone() {
		return isTombstone;
	}

	public ChatOperation invert() {
		return new ChatOperation(entry, !isTombstone);
	}
}
