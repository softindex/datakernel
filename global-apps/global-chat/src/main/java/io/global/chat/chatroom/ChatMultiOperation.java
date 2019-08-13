package io.global.chat.chatroom;

import io.global.ot.name.ChangeName;
import io.global.ot.set.SetOperation;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public final class ChatMultiOperation {
	private final List<SetOperation<Message>> messageOps;
	private final List<ChangeName> roomNameOps;

	public ChatMultiOperation(List<SetOperation<Message>> messageOps, List<ChangeName> roomNameOps) {
		this.messageOps = messageOps;
		this.roomNameOps = roomNameOps;
	}

	public static ChatMultiOperation create() {
		return new ChatMultiOperation(new ArrayList<>(), new ArrayList<>());
	}

	@SafeVarargs
	public final ChatMultiOperation withMessageOps(SetOperation<Message>... messageOps) {
		this.messageOps.addAll(asList(messageOps));
		return this;
	}

	public ChatMultiOperation withRoomNameOps(ChangeName... roomNameOps) {
		this.roomNameOps.addAll(asList(roomNameOps));
		return this;
	}

	public List<SetOperation<Message>> getMessageOps() {
		return messageOps;
	}

	public List<ChangeName> getRoomNameOps() {
		return roomNameOps;
	}
}
