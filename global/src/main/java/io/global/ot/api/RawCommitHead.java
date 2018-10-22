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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.ByteArrayIdentity;
import io.global.common.RepoID;

import java.util.Arrays;

import static io.global.ot.util.BinaryDataFormats.*;

public final class RawCommitHead implements ByteArrayIdentity {
	public final byte[] bytes;

	public final RepoID repositoryId;
	public final CommitId commitId;
	public final long timestamp;

	private RawCommitHead(byte[] bytes,
			RepoID repositoryId, CommitId commitId, long timestamp) {
		this.bytes = bytes;
		this.repositoryId = repositoryId;
		this.commitId = commitId;
		this.timestamp = timestamp;
	}

	public static RawCommitHead ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		RepoID repositoryId = readRepoID(buf);
		CommitId commitId = readCommitId(buf);
		long timestamp = buf.readLong();
		return new RawCommitHead(bytes, repositoryId, commitId, timestamp);
	}

	public static RawCommitHead of(RepoID repositoryId, CommitId commitId, long timestamp) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(repositoryId) + sizeof(commitId) + 8);
		writeRepoID(buf, repositoryId);
		writeCommitId(buf, commitId);
		buf.writeLong(timestamp);
		return new RawCommitHead(buf.asArray(),
				repositoryId, commitId, timestamp);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public RepoID getRepositoryId() {
		return repositoryId;
	}

	public CommitId getCommitId() {
		return commitId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawCommitHead that = (RawCommitHead) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
