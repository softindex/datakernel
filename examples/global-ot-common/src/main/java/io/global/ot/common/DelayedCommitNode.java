/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.global.ot.common;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNode;
import io.global.ot.api.CommitId;

import java.util.List;

public final class DelayedCommitNode<D> implements OTNode<CommitId, D, OTCommit<CommitId, D>> {
	private final OTNode<CommitId, D, OTCommit<CommitId, D>> node;
	private final int delay;

	public DelayedCommitNode(OTNode<CommitId, D, OTCommit<CommitId, D>> node, int delay) {
		this.node = node;
		this.delay = delay;
	}

	@Override
	public Promise<OTCommit<CommitId, D>> createCommit(CommitId parent, List<? extends D> diffs, long level) {
		SettablePromise<OTCommit<CommitId, D>> promise = new SettablePromise<>();
		long pushAt = System.currentTimeMillis() + delay;
		Eventloop.getCurrentEventloop().schedule(pushAt,
				() -> node.createCommit(parent, diffs, level).whenComplete(promise::set));
		return promise;
	}

	@Override
	public Promise<CommitId> push(OTCommit<CommitId, D> commit) {
		return node.push(commit);
	}

	@Override
	public Promise<FetchData<CommitId, D>> checkout() {
		return node.checkout();
	}

	@Override
	public Promise<FetchData<CommitId, D>> fetch(CommitId currentCommitId) {
		return node.fetch(currentCommitId);
	}

}
