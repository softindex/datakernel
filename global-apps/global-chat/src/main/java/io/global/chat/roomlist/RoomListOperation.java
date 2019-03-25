package io.global.chat.roomlist;

import static java.util.Collections.emptySet;

public class RoomListOperation {
	public static final RoomListOperation EMPTY = new RoomListOperation(new Room("", emptySet()), true);

	private final Room room;
	private final boolean remove;

	public RoomListOperation(Room room, boolean remove) {
		this.room = room;
		this.remove = remove;
	}

	public static RoomListOperation create(Room room) {
		return new RoomListOperation(room, false);
	}

	public static RoomListOperation delete(Room room) {
		return new RoomListOperation(room, true);
	}

	public RoomListOperation invert() {
		return new RoomListOperation(room, !remove);
	}

	public boolean isEmpty() {
		return room.getParticipants().isEmpty();
	}

	public boolean isRemove() {
		return remove;
	}

	public Room getRoom() {
		return room;
	}

	public boolean isInversionFor(RoomListOperation other) {
		return room.equals(other.room) && remove != other.remove;
	}
}
