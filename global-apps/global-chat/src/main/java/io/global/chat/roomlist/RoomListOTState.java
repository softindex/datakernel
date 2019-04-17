package io.global.chat.roomlist;

import io.datakernel.ot.OTState;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public final class RoomListOTState implements OTState<RoomListOperation> {
	private static final Consumer<RoomListOperation> NO_ACTION = op -> {};

	private final Set<Room> rooms = new HashSet<>();
	private Consumer<RoomListOperation> listener = NO_ACTION;

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
		listener.accept(op);
	}

	public Set<Room> getRooms() {
		return rooms;
	}

	public void setListener(Consumer<RoomListOperation> listener) {
		this.listener = listener;
	}
}
