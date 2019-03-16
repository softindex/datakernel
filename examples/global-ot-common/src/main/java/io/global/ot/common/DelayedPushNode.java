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

import java.time.Duration;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;

public final class DelayedPushNode<D> implements OTNode<CommitId, D, OTCommit<CommitId, D>> {
	private final OTNode<CommitId, D, OTCommit<CommitId, D>> node;
	private final long delay;

	private DelayedPushNode(OTNode<CommitId, D, OTCommit<CommitId, D>> node, long delay) {
		this.node = node;
		this.delay = delay;
	}

	public static <D> DelayedPushNode<D> create(OTNode<CommitId, D, OTCommit<CommitId, D>> node, Duration delay) {
		checkArgument(delay.toMillis() >= 0, "Delay cannot be a negative value");
		return new DelayedPushNode<>(node, delay.toMillis());
	}

	@Override
	public Promise<OTCommit<CommitId, D>> createCommit(CommitId parent, List<? extends D> diffs, long level) {
		return node.createCommit(parent, diffs, level);
	}

	@Override
	public Promise<FetchData<CommitId, D>> push(OTCommit<CommitId, D> commit) {
		if (delay != 0) {
			SettablePromise<FetchData<CommitId, D>> promise = new SettablePromise<>();
			Eventloop.getCurrentEventloop().delay(delay, () -> node.push(commit).acceptEx(promise::set));
			return promise;
		} else {
			return node.push(commit);
		}
	}

	@Override
	public Promise<FetchData<CommitId, D>> checkout() {
		return node.checkout();
	}

	@Override
	public Promise<FetchData<CommitId, D>> fetch(CommitId currentCommitId) {
		return node.fetch(currentCommitId);
	}

	@Override
	public Promise<FetchData<CommitId, D>> poll(CommitId currentCommitId) {
		return node.poll(currentCommitId);
	}
}
