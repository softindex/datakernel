package io.global.chat.chatroom;

import io.global.chat.chatroom.messages.MessageOperation;
import io.global.ot.name.ChangeName;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public final class ChatMultiOperation {
	private final List<MessageOperation> messageOps;
	private final List<ChangeName> roomNameOps;

	public ChatMultiOperation(List<MessageOperation> messageOps, List<ChangeName> roomNameOps) {
		this.messageOps = messageOps;
		this.roomNameOps = roomNameOps;
	}

	public static ChatMultiOperation create() {
		return new ChatMultiOperation(new ArrayList<>(), new ArrayList<>());
	}

	public ChatMultiOperation withMessageOps(MessageOperation... messageOps) {
		this.messageOps.addAll(asList(messageOps));
		return this;
	}

	public ChatMultiOperation withRoomNameOps(ChangeName... roomNameOps) {
		this.roomNameOps.addAll(asList(roomNameOps));
		return this;
	}

	public List<MessageOperation> getMessageOps() {
		return messageOps;
	}

	public List<ChangeName> getRoomNameOps() {
		return roomNameOps;
	}
}
