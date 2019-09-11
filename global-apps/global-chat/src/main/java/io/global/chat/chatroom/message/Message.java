package io.global.chat.chatroom.message;

import io.global.common.PubKey;

public final class Message {
	private final MessageType messageType;
	private final long timestamp;
	private final PubKey author;
	private final String content;

	public Message(MessageType messageType, long timestamp, PubKey author, String content) {
		this.messageType = messageType;
		this.timestamp = timestamp;
		this.author = author;
		this.content = content;
	}

	public MessageType getMessageType() {
		return messageType;
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

	public boolean isEmpty() {
		return content.equals("");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Message message = (Message) o;

		if (timestamp != message.timestamp) return false;
		if (messageType != message.messageType) return false;
		if (!author.equals(message.author)) return false;
		if (!content.equals(message.content)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = messageType.hashCode();
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + author.hashCode();
		result = 31 * result + content.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Message{" +
				"messageType=" + messageType +
				", timestamp=" + timestamp +
				", author=" + author +
				", content='" + content + '\'' +
				'}';
	}
}

