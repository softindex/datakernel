package io.global.fs.app.container;

import io.global.common.PubKey;
import io.global.common.SharedSimKey;

import java.util.Set;

public class SharedDirMetadata {
	private final String dirId;
	private final String dirName;
	private final Set<PubKey> participants;
	private final SharedSimKey sharedSimKey;

	public SharedDirMetadata(String dirId, String dirName, Set<PubKey> participants, SharedSimKey sharedSimKey) {
		this.dirId = dirId;
		this.dirName = dirName;
		this.participants = participants;
		this.sharedSimKey = sharedSimKey;
	}

	public String getDirId() {
		return dirId;
	}

	public String getDirName() {
		return dirName;
	}

	public Set<PubKey> getParticipants() {
		return participants;
	}

	public SharedSimKey getSharedSimKey() {
		return sharedSimKey;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SharedDirMetadata that = (SharedDirMetadata) o;

		if (!dirId.equals(that.dirId)) return false;
		if (!dirName.equals(that.dirName)) return false;
		if (!participants.equals(that.participants)) return false;
		if (!sharedSimKey.equals(that.sharedSimKey)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = dirId.hashCode();
		result = 31 * result + dirName.hashCode();
		result = 31 * result + participants.hashCode();
		result = 31 * result + sharedSimKey.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "SharedDirMetadata{" +
				"dirId='" + dirId + '\'' +
				", dirName='" + dirName + '\'' +
				", participants=" + participants +
				", sharedSimKey=" + sharedSimKey +
				'}';
	}
}
