package io.global.ot.shared;

import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Set;

import static io.datakernel.common.collection.CollectionUtils.keysToMap;

public final class SharedRepo {
	@NotNull
	private final String id;
	@NotNull
	private final Map<PubKey, Boolean> participants;
	@NotNull
	private String name;

	public SharedRepo(@NotNull String id, @NotNull String name, Set<PubKey> participants) {
		this.id = id;
		this.name = name;
		this.participants = keysToMap(participants, $ -> false);
	}

	@NotNull
	public String getId() {
		return id;
	}

	@NotNull
	public String getName() {
		return name;
	}

	public Set<PubKey> getParticipants() {
		return participants.keySet();
	}

	public void setName(@NotNull String name) {
		this.name = name;
	}

	public void setMessageSent(PubKey participant) {
		participants.computeIfPresent(participant, ($1, $2) -> true);
	}

	public boolean isMessageSent(PubKey participant) {
		return participants.get(participant);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SharedRepo that = (SharedRepo) o;

		if (!id.equals(that.id)) return false;
		if (!name.equals(that.name)) return false;
		if (!participants.keySet().equals(that.participants.keySet())) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = id.hashCode();
		result = 31 * result + name.hashCode();
		result = 31 * result + participants.keySet().hashCode();
		return result;
	}
}
