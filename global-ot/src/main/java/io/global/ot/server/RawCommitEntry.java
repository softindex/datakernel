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

package io.global.ot.server;

import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;

final class RawCommitEntry implements Comparable<RawCommitEntry> {
	public final CommitId commitId;
	public final RawCommit commit;

	public RawCommitEntry(CommitId commitId, RawCommit commit) {
		this.commitId = commitId;
		this.commit = commit;
	}

	public CommitId getCommitId() {
		return commitId;
	}

	public RawCommit getCommit() {
		return commit;
	}

	@Override
	public int compareTo(RawCommitEntry other) {
		return Long.compare(this.commit.getLevel(), other.commit.getLevel());
	}
}
