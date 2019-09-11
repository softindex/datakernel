package io.global.chat.chatroom.operation;

import io.global.chat.chatroom.ChatRoomOTState;
import io.global.chat.chatroom.message.Message;
import io.global.common.PubKey;

import static io.global.chat.Utils.STUB_PUB_KEY;
import static io.global.chat.Utils.toRegularMessage;

public final class MessageOperation implements ChatRoomOperation {
	public static final MessageOperation EMPTY = new MessageOperation(-1, STUB_PUB_KEY, "", false);

	private final long timestamp;
	private final PubKey author;
	private final String content;
	private final boolean invert;

	public MessageOperation(long timestamp, PubKey author, String content, boolean invert) {
		this.timestamp = timestamp;
		this.author = author;
		this.content = content;
		this.invert = invert;
	}

	public static MessageOperation insert(long timestamp, PubKey author, String content) {
		return new MessageOperation(timestamp, author, content, false);
	}

	public static MessageOperation delete(long timestamp, PubKey author, String content) {
		return new MessageOperation(timestamp, author, content, true);
	}

	@Override
	public void apply(ChatRoomOTState state) {
		Message message = toRegularMessage(this);
		if (invert) {
			assert state.getMessages().contains(message);
			state.removeMessage(message);
		} else {
			assert !state.getMessages().contains(message);
			state.addMessage(message);
		}
	}

	public long getTimestamp() {
		return timestamp;
	}

	public PubKey getAuthor() {
		return author;
	}

	public String getContent() {
		return content;
	}

	public boolean isInvert() {
		return invert;
	}

	@Override
	public boolean isEmpty() {
		return content.isEmpty();
	}

	@Override
	public MessageOperation invert() {
		return new MessageOperation(timestamp, author, content, !invert);
	}

	public boolean isInversionFor(MessageOperation other) {
		return timestamp == other.timestamp &&
				author.equals(other.author) &&
				content.equals(other.content) &&
				invert != other.invert;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		MessageOperation that = (MessageOperation) o;

		if (timestamp != that.timestamp) return false;
		if (invert != that.invert) return false;
		if (!author.equals(that.author)) return false;
		if (!content.equals(that.content)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + author.hashCode();
		result = 31 * result + content.hashCode();
		result = 31 * result + (invert ? 1 : 0);
		return result;
	}

	// @Override
	// public String toString() {
	// 	return '{' + (invert ? "-" : "+") + '}';
	// }


	@Override
	public String toString() {
		return (invert ? '-' : '+') + "{" +
				"timestamp=" + timestamp +
				", author=" + author +
				", content='" + content +
				"\'}";
	}
}
