package io.global.chat.roomlist;

import io.global.common.PubKey;

import java.util.Set;

public final class Room {
	private final String id;
	private final Set<PubKey> participants;

	public Room(String id, Set<PubKey> participants) {
		this.id = id;
		this.participants = participants;
	}

	public String getId() {
		return id;
	}

	public Set<PubKey> getParticipants() {
		return participants;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Room room = (Room) o;

		if (!id.equals(room.id)) return false;
		if (!participants.equals(room.participants)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = id.hashCode();
		result = 31 * result + participants.hashCode();
		return result;
	}
}
