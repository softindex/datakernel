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

	public final static class ChatEntry {
		private final long timestamp;
		private final String author;
		private final String content;

		public ChatEntry(long timestamp, @NotNull String author, @NotNull String content) {
			this.timestamp = timestamp;
			this.author = author;
			this.content = content;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public String getAuthor() {
			return author;
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
			if (!author.equals(chatEntry.author)) return false;
			if (!content.equals(chatEntry.content)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + author.hashCode();
			result = 31 * result + content.hashCode();
			return result;
		}
	}

	public Set<ChatEntry> getChatEntries() {
		return Collections.unmodifiableSet(entries);
	}
}
