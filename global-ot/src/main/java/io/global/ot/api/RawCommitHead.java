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

import io.datakernel.common.parse.ParseException;

import static io.datakernel.common.Preconditions.checkNotNull;

public final class RawCommitHead {
	private final RepoID repositoryId;
	private final CommitId commitId;
	private final long timestamp;

	private RawCommitHead(RepoID repositoryId, CommitId commitId, long timestamp) {
		this.repositoryId = checkNotNull(repositoryId);
		this.commitId = checkNotNull(commitId);
		this.timestamp = timestamp;
	}

	public static RawCommitHead parse(RepoID repositoryId, CommitId commitId, long timestamp) throws ParseException {
		return of(repositoryId, commitId, timestamp);
	}

	public static RawCommitHead of(RepoID repositoryId, CommitId commitId, long timestamp) {
		return new RawCommitHead(repositoryId, commitId, timestamp);
	}

	public RepoID getRepositoryId() {
		return repositoryId;
	}

	public CommitId getCommitId() {
		return commitId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawCommitHead that = (RawCommitHead) o;
		if (timestamp != that.timestamp) return false;
		if (!repositoryId.equals(that.repositoryId)) return false;
		return commitId.equals(that.commitId);
	}

	@Override
	public int hashCode() {
		int result = repositoryId.hashCode();
		result = 31 * result + commitId.hashCode();
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "RawCommitHead{" +
				"repositoryId=" + repositoryId +
				", commitId=" + commitId +
				", timestamp=" + timestamp +
				'}';
	}
}
