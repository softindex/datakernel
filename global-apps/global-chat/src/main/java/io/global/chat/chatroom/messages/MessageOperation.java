package io.global.chat.chatroom.messages;

import io.global.chat.chatroom.ChatRoomOperation;

public final class MessageOperation implements ChatRoomOperation {
	public static final MessageOperation EMPTY = new MessageOperation(new Message(0, "", ""), false);

	private final Message message;
	private final boolean isTombstone;

	public MessageOperation(Message message, boolean remove) {
		this.message = message;
		this.isTombstone = remove;
	}

	public static MessageOperation insert(Message message) {
		return new MessageOperation(message, false);
	}

	public static MessageOperation delete(Message message) {
		return new MessageOperation(message, true);
	}

	public Message getMessage() {
		return message;
	}

	public boolean isTombstone() {
		return isTombstone;
	}

	public boolean isEmpty() {
		return message.isEmpty();
	}

	public MessageOperation invert() {
		return new MessageOperation(message, !isTombstone);
	}

	public boolean isInversionFor(MessageOperation other) {
		return message.equals(other.getMessage())
				&& isTombstone != other.isTombstone;
	}

	@Override
	public String toString() {
		return '{' + (isTombstone ? "-" : "+") + message + '}';
	}
}
