package io.global.chat.chatroom;

import io.global.chat.chatroom.messages.MessageOperation;
import io.global.chat.chatroom.roomname.ChangeRoomName;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public final class ChatMultiOperation {
	private final List<MessageOperation> messageOps;
	private final List<ChangeRoomName> roomNameOps;

	public ChatMultiOperation(List<MessageOperation> messageOps, List<ChangeRoomName> roomNameOps) {
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

	public ChatMultiOperation withRoomNameOps(ChangeRoomName... roomNameOps) {
		this.roomNameOps.addAll(asList(roomNameOps));
		return this;
	}

	public List<MessageOperation> getMessageOps() {
		return messageOps;
	}

	public List<ChangeRoomName> getRoomNameOps() {
		return roomNameOps;
	}
}
