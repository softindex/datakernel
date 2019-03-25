package io.global.chat.roomlist;

import io.datakernel.ot.OTState;

import java.util.HashSet;
import java.util.Set;

public final class RoomListOTState implements OTState<RoomListOperation> {
	private final Set<Room> rooms = new HashSet<>();

	@Override
	public void init() {
		rooms.clear();
	}

	@Override
	public void apply(RoomListOperation op) {
		if (op.isEmpty()) return;

		if (op.isRemove()) {
			rooms.remove(op.getRoom());
		} else {
			rooms.add(op.getRoom());
		}
	}

	public Set<Room> getRooms() {
		return rooms;
	}
}
