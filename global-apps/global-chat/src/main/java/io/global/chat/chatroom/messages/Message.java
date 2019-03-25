package io.global.chat.chatroom.messages;

public class Message {
	private final long timestamp;
	private final String author;
	private final String content;

	public Message(long timestamp, String author, String content) {
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

	public boolean isEmpty() {
		return content.equals("");
	}

	public boolean equalsWithoutTimestamp(Message other) {
		if (!author.equals(other.author)) return false;
		if (!content.equals(other.content)) return false;

		return true;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Message message = (Message) o;

		if (timestamp != message.timestamp) return false;
		if (!author.equals(message.author)) return false;
		if (!content.equals(message.content)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + author.hashCode();
		result = 31 * result + content.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "{[" + timestamp + "] " + author + " : " + content + '}';
	}
}
