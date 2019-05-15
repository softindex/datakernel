/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.ot.api;

import org.jetbrains.annotations.NotNull;

import java.util.Set;

public final class CommitEntry implements Comparable<CommitEntry> {
	private final CommitId commitId;
	private final RawCommit commit;

	public CommitEntry(CommitId commitId, RawCommit commit) {
		this.commitId = commitId;
		this.commit = commit;
	}

	public CommitId getCommitId() {
		return commitId;
	}

	public RawCommit getCommit() {
		return commit;
	}

	public long getLevel() {
		return commit.getLevel();
	}

	@NotNull
	public Set<CommitId> getParents() {
		return commit.getParents();
	}

	@Override
	public int compareTo(CommitEntry other) {
		return commitId.compareTo(other.getCommitId());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CommitEntry entry = (CommitEntry) o;
		return commitId.equals(entry.commitId);
	}

	@Override
	public int hashCode() {
		return commitId.hashCode();
	}
}
