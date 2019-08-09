package io.global.chat.chatroom;

import io.datakernel.async.Promise;
import io.datakernel.ot.OTState;
import io.global.chat.chatroom.messages.Message;
import io.global.ot.name.ChangeName;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static io.datakernel.util.CollectionUtils.getLast;

public final class ChatRoomOTState implements OTState<ChatMultiOperation> {
	private String roomName;
	private Set<Message> messages;

	@Override
	public Promise<Void> init() {
		roomName = "";
		messages = new TreeSet<>(Comparator.comparingLong(Message::getTimestamp));
		return Promise.complete();
	}

	@Override
	public Promise<Void> apply(ChatMultiOperation multiOperation) {
		multiOperation.getMessageOps().forEach(op -> {
			if (op.isEmpty()) return;
			if (op.isTombstone()) {
				messages.remove(op.getMessage());
			} else {
				messages.add(op.getMessage());
			}
		});
		List<ChangeName> roomNameOps = multiOperation.getRoomNameOps();
		if (!roomNameOps.isEmpty()) {
			roomName = getLast(roomNameOps).getNext();
		}
		return Promise.complete();
	}

	public String getRoomName() {
		return roomName;
	}

	public Set<Message> getMessages() {
		return messages;
	}

	public boolean isEmpty() {
		return roomName.equals("") && messages.isEmpty();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ChatRoomOTState that = (ChatRoomOTState) o;

		if (!roomName.equals(that.roomName)) return false;
		if (!messages.equals(that.messages)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return 31 * roomName.hashCode() + messages.hashCode();
	}

	@Override
	public String toString() {
		return "ChatRoomOTState{" +
				"roomName='" + roomName + '\'' +
				", messages=" + messages +
				'}';
	}
}
