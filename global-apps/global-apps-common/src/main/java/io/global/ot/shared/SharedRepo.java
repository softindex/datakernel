package io.global.ot.shared;

import io.global.common.PubKey;

import java.util.Map;
import java.util.Set;

import static io.datakernel.util.CollectionUtils.keysToMap;

public final class SharedRepo {
	private final String id;
	private final Map<PubKey, Boolean> participants;

	public SharedRepo(String id, Set<PubKey> participants) {
		this.id = id;
		this.participants = keysToMap(participants, $ -> false);
	}

	public String getId() {
		return id;
	}

	public Set<PubKey> getParticipants() {
		return participants.keySet();
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

		SharedRepo sharedRepo = (SharedRepo) o;

		if (!id.equals(sharedRepo.id)) return false;
		if (!participants.keySet().equals(sharedRepo.participants.keySet())) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = id.hashCode();
		result = 31 * result + participants.keySet().hashCode();
		return result;
	}
}
