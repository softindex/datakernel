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

package io.datakernel.ot;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.exception.UncheckedException;
import io.datakernel.ot.exceptions.OTTransformException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.async.AsyncSuppliers.resubscribe;
import static io.datakernel.async.Promises.sequence;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> {
	private static final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final OTSystem<D> otSystem;
	private final OTNode<K, D> repository;

	private OTState<D> state;

	@Nullable
	private K localRevision;
	private long localRevisionLevel;

	@Nullable
	private K remoteRevision;
	private long remoteRevisionLevel;

	private List<D> workingDiffs = new ArrayList<>();
	private Map<K, OTCommit<K, D>> pendingCommits = new HashMap<>();

	public OTStateManager(OTSystem<D> otSystem, OTNode<K, D> repository, OTState<D> state) {
		this.otSystem = otSystem;
		this.repository = repository;
		this.state = state;
	}

	public Promise<Void> checkout() {
		return repository.checkout()
				.whenResult(checkoutData -> {
					state.init();
					apply(checkoutData.getDiffs());

					workingDiffs.clear();
					pendingCommits.clear();

					localRevision = remoteRevision = checkoutData.getCommitId();
					localRevisionLevel = remoteRevisionLevel = checkoutData.getLevel();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncSupplier<Void> pull = resubscribe(this::doPull);

	public Promise<Void> pull() {
		checkState(isValid());
		return pull.get();
	}

	private Promise<Void> doPull() {
		checkState(isValid());
		return repository.fetch(remoteRevision)
				.whenResult(fetchData -> {
					checkState(isValid());
					List<D> fetchedDiffs = fetchData.getDiffs();

					TransformResult<D> transformed;
					try {
						transformed = otSystem.transform(
								otSystem.squash(workingDiffs),
								otSystem.squash(fetchedDiffs));
					} catch (OTTransformException e) {
						invalidateInternalState();
						throw new UncheckedException(e);
					}

					apply(transformed.left);
					workingDiffs = new ArrayList<>(transformed.right);

					localRevision = remoteRevision = fetchData.getCommitId();
					localRevisionLevel = remoteRevisionLevel = fetchData.getLevel();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public Promise<Void> sync() {
		return sequence(this::pull, this::commit, this::push)
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncSupplier<Void> commit = resubscribe(this::doCommit);

	public Promise<Void> commit() {
		return commit.get();
	}

	@NotNull
	private Promise<Void> doCommit() {
		if (workingDiffs.isEmpty()) {
			return Promise.complete();
		}
		K revisionCopy = localRevision;
		List<D> workingDiffsCopy = new ArrayList<>(workingDiffs);
		workingDiffs = new ArrayList<>();
		return repository.createCommit(revisionCopy, otSystem.squash(workingDiffsCopy), localRevisionLevel + 1L)
				.whenResult(commit -> {
					pendingCommits.put(commit.getId(), commit);

					localRevision = commit.getId();
					localRevisionLevel = commit.getLevel();
					workingDiffs = new ArrayList<>();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncSupplier<Void> push = resubscribe(this::doPush);

	public Promise<Void> push() {
		return push.get();
	}

	private Promise<Void> doPush() {
		List<OTCommit<K, D>> list = new ArrayList<>(pendingCommits.values());
		K revisionCopy = localRevision;
		return repository.pushAll(list)
				.whenResult($ -> {
					list.forEach(commit -> pendingCommits.remove(commit.getId()));
					remoteRevision = revisionCopy;
				})
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public void add(D diff) {
		checkState(isValid());
		addAll(singletonList(diff));
	}

	public void addAll(List<D> diffs) {
		checkState(isValid());
		try {
			for (D diff : diffs) {
				if (!otSystem.isEmpty(diff)) {
					workingDiffs.add(diff);
					state.apply(diff);
				}
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void apply(List<D> diffs) {
		try {
			for (D op : diffs) {
				state.apply(op);
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	@SuppressWarnings("AssignmentToNull") // state is invalid, no further calls should be made
	private void invalidateInternalState() {
		state = null;

		localRevision = null;
		localRevisionLevel = 0;
		workingDiffs = null;

		pendingCommits = null;
	}

	public K getLocalRevision() {
		return checkNotNull(localRevision, "Internal state has been invalidated");
	}

	public OTState<D> getState() {
		return state;
	}

	public boolean isValid() {
		return localRevision != null;
	}

	public boolean hasWorkingDiffs() {
		return !workingDiffs.isEmpty();
	}

	public boolean hasPendingCommits() {
		return !pendingCommits.isEmpty();
	}

	@Override
	public String toString() {
		return "{" +
				"revision=" + localRevision +
				" workingDiffs:" + (workingDiffs != null ? workingDiffs.size() : null) +
				" pendingCommits:" + (pendingCommits != null ? pendingCommits.size() : null) +
				'}';
	}
}
