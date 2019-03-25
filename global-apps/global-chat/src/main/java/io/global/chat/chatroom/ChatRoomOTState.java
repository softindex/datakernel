package io.global.chat.chatroom;

import io.datakernel.ot.OTState;
import io.global.chat.chatroom.messages.Message;
import io.global.chat.chatroom.roomname.ChangeRoomName;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static io.datakernel.util.CollectionUtils.getLast;

public final class ChatRoomOTState implements OTState<ChatMultiOperation> {
	private String roomName;
	private Set<Message> messages;

	@Override
	public void init() {
		roomName = "";
		messages = new TreeSet<>(Comparator.comparingLong(Message::getTimestamp));
	}

	@Override
	public void apply(ChatMultiOperation multiOperation) {
		multiOperation.getMessageOps().forEach(op -> {
			if (op.isEmpty()) return;
			if (op.isTombstone()) {
				messages.remove(op.getMessage());
			} else {
				messages.add(op.getMessage());
			}
		});
		List<ChangeRoomName> roomNameOps = multiOperation.getRoomNameOps();
		if (!roomNameOps.isEmpty()) {
			roomName = getLast(roomNameOps).getNext();
		}
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
		int result = roomName.hashCode();
		result = 31 * result + messages.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "ChatRoomOTState{" +
				"roomName='" + roomName + '\'' +
				", messages=" + messages +
				'}';
	}
}
