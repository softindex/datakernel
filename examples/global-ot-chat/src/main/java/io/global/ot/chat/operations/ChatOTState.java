package io.global.ot.chat.operations;

import io.datakernel.ot.OTState;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class ChatOTState implements OTState<ChatOperation> {
	private final Set<ChatEntry> entries = new TreeSet<>(Comparator.comparingLong(ChatEntry::getTimestamp));

	@Override
	public void init() {
		entries.clear();
	}

	@Override
	public void apply(ChatOperation op) {
		op.apply(entries);
	}

	public static class ChatEntry {
		private final long timestamp;
		private final String content;

		public ChatEntry(long timestamp, @NotNull String content) {
			this.timestamp = timestamp;
			this.content = content;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public String getContent() {
			return content;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ChatEntry chatEntry = (ChatEntry) o;

			if (timestamp != chatEntry.timestamp) return false;
			if (!content.equals(chatEntry.content)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + content.hashCode();
			return result;
		}
	}

	public Set<ChatEntry> getChatEntries() {
		return Collections.unmodifiableSet(entries);
	}
}
