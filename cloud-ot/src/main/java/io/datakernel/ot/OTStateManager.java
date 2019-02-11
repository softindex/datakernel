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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.UncheckedException;
import io.datakernel.ot.OTNode.ProtoCommit;
import io.datakernel.ot.exceptions.OTTransformException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.AsyncSuppliers.resubscribe;
import static io.datakernel.async.Promises.sequence;
import static io.datakernel.util.CollectionUtils.isShallowEquals;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTNode<K, D> repository;

	private OTState<D> state;

	@Nullable
	private K revision;
	private long level;

	private List<D> workingDiffs = new ArrayList<>();

	@Nullable
	private ProtoCommit<K> pendingCommit;

	public OTStateManager(Eventloop eventloop, OTSystem<D> otSystem, OTNode<K, D> repository, OTState<D> state) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.repository = repository;
		this.state = state;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}


	@NotNull
	@Override
	public Promise<Void> start() {
		return checkout();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return isValid() ?
				sync().whenComplete(($, e) -> invalidateInternalState()) :
				Promise.complete();
	}

	@NotNull
	public Promise<Void> checkout() {
		checkState(revision == null);
		return repository.checkout()
				.whenResult(checkoutData -> {
					state.init();
					apply(checkoutData.getDiffs());

					workingDiffs.clear();

					revision = checkoutData.getCommitId();
					level = checkoutData.getLevel();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncSupplier<Void> sync = resubscribe(this::doSync);

	@NotNull
	public Promise<Void> sync() {
		return sync.get();
	}

	@NotNull
	private Promise<Void> doSync() {
		checkState(isValid());
		return sequence(
				pendingCommit == null ? Promise::complete : this::push,
				this::pull, this::commit, this::push)
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@NotNull
	private Promise<Void> pull() {
		assert pendingCommit == null;
		return repository.fetch(revision)
				.whenResult(fetchData -> {
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

					revision = fetchData.getCommitId();
					level = fetchData.getLevel();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@NotNull
	private Promise<Void> commit() {
		assert pendingCommit == null;
		if (workingDiffs.isEmpty()) return Promise.complete();
		List<D> workingDiffsCopy = new ArrayList<>(workingDiffs);
		return repository.createCommit(revision, otSystem.squash(workingDiffsCopy), level + 1L)
				.whenResult(commit -> {
					assert pendingCommit == null;
					pendingCommit = commit;
					assert isShallowEquals(workingDiffs.subList(0, workingDiffsCopy.size()), workingDiffsCopy);
					workingDiffs = new ArrayList<>(workingDiffs.subList(workingDiffsCopy.size(), workingDiffs.size()));
					revision = commit.getCommitId();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@NotNull
	private Promise<Void> push() {
		if (pendingCommit == null) return Promise.complete();
		return repository.push(pendingCommit)
				.whenResult($ -> pendingCommit = null)
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public void add(@NotNull D diff) {
		checkState(isValid());
		addAll(singletonList(diff));
	}

	public void addAll(@NotNull List<D> diffs) {
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

		revision = null;
		level = 0;
		workingDiffs = null;

		pendingCommit = null;
	}

	public K getRevision() {
		return checkNotNull(revision, "Internal state has been invalidated");
	}

	public OTState<D> getState() {
		return state;
	}

	public List<D> getWorkingDiffs() {
		return new ArrayList<>(workingDiffs);
	}

	public boolean isValid() {
		return revision != null;
	}

	public boolean hasWorkingDiffs() {
		return !workingDiffs.isEmpty();
	}

	public boolean hasPendingCommits() {
		return pendingCommit != null;
	}

	@Override
	public String toString() {
		return "{" +
				"revision=" + revision +
				" workingDiffs:" + (workingDiffs != null ? workingDiffs.size() : null) +
				" pendingCommits:" + (pendingCommit != null) +
				"}";
	}
}
