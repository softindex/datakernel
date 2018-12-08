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

import io.datakernel.exception.ParseException;
import io.global.common.Hash;
import io.global.common.api.EncryptedData;

import java.util.Objects;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class RawSnapshot {
	public final RepoID repositoryId;
	public final CommitId commitId;
	public final EncryptedData encryptedDiffs;
	public final Hash simKeyHash;

	private RawSnapshot(RepoID repositoryId, CommitId commitId, EncryptedData encryptedDiffs, Hash simKeyHash) {
		this.repositoryId = checkNotNull(repositoryId);
		this.commitId = checkNotNull(commitId);
		this.encryptedDiffs = checkNotNull(encryptedDiffs);
		this.simKeyHash = checkNotNull(simKeyHash);
	}

	public static RawSnapshot of(RepoID repositoryId,
			CommitId commitId,
			EncryptedData encryptedDiffs,
			Hash simKeyHash) {
		return new RawSnapshot(repositoryId, commitId, encryptedDiffs, simKeyHash);
	}

	public static RawSnapshot parse(RepoID repositoryId,
			CommitId commitId,
			EncryptedData encryptedDiffs,
			Hash simKeyHash) throws ParseException {
		return new RawSnapshot(repositoryId, commitId, encryptedDiffs, simKeyHash);
	}

	public RepoID getRepositoryId() {
		return repositoryId;
	}

	public CommitId getCommitId() {
		return checkNotNull(commitId);
	}

	public EncryptedData getEncryptedDiffs() {
		return checkNotNull(encryptedDiffs);
	}

	public Hash getSimKeyHash() {
		return checkNotNull(simKeyHash);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawSnapshot that = (RawSnapshot) o;
		return Objects.equals(repositoryId, that.repositoryId) &&
				Objects.equals(commitId, that.commitId) &&
				Objects.equals(encryptedDiffs, that.encryptedDiffs) &&
				Objects.equals(simKeyHash, that.simKeyHash);
	}

	@Override
	public int hashCode() {
		return Objects.hash(repositoryId, commitId, encryptedDiffs, simKeyHash);
	}
}
