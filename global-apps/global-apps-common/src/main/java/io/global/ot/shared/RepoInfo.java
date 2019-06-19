package io.global.ot.shared;

import io.global.common.PubKey;

import java.util.Set;

public final class RepoInfo {
	private final String name;
	private final Set<PubKey> participants;
	private final boolean remove;

	public RepoInfo(String name, Set<PubKey> participants, boolean remove) {
		this.participants = participants;
		this.name = name;
		this.remove = remove;
	}

	public String getName() {
		return name;
	}

	public Set<PubKey> getParticipants() {
		return participants;
	}

	public boolean isRemove() {
		return remove;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RepoInfo repoInfo = (RepoInfo) o;

		if (remove != repoInfo.remove) return false;
		if (!participants.equals(repoInfo.participants)) return false;
		if (!name.equals(repoInfo.name)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = participants.hashCode();
		result = 31 * result + name.hashCode();
		result = 31 * result + (remove ? 1 : 0);
		return result;
	}
}
