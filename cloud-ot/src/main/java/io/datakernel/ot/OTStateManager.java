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

import io.datakernel.async.AsyncExecutors;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.AsyncSuppliers.AsyncSupplierWithStatus;
import io.datakernel.async.Promise;
import io.datakernel.async.RetryPolicy;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.exception.UncheckedException;
import io.datakernel.ot.OTNode.FetchData;
import io.datakernel.ot.exceptions.OTTransformException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.coalesce;
import static io.datakernel.async.Promises.sequence;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTNode<K, D, Object> repository;

	private OTState<D> state;

	@Nullable
	private K commitId;
	private long level;

	private List<D> workingDiffs = new ArrayList<>();

	@Nullable
	private Object pendingCommit;
	private List<D> pendingCommitDiffs;

	private final AsyncSupplierWithStatus<Void> sync = new AsyncSupplierWithStatus<>(coalesce(this::doSync));

	@Nullable
	private AsyncSupplierWithStatus<Void> poll;

	@SuppressWarnings("unchecked")
	private OTStateManager(Eventloop eventloop, OTSystem<D> otSystem, OTNode<K, D, ?> repository, OTState<D> state) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.repository = (OTNode<K, D, Object>) repository;
		this.state = state;
	}

	@NotNull
	public static <K, D> OTStateManager<K, D> create(@NotNull Eventloop eventloop, @NotNull OTSystem<D> otSystem, @NotNull OTNode<K, D, ?> repository,
			@NotNull OTState<D> state) {
		return new OTStateManager<>(eventloop, otSystem, repository, state);
	}

	@NotNull
	public OTStateManager<K, D> withPoll() {
		return withPoll(Function.identity());
	}

	@NotNull
	public OTStateManager<K, D> withPoll(@NotNull RetryPolicy pollRetryPolicy) {
		return withPoll(poll -> poll.withExecutor(AsyncExecutors.retry(pollRetryPolicy)));
	}

	@NotNull
	public OTStateManager<K, D> withPoll(@NotNull Function<AsyncSupplier<Void>, AsyncSupplier<Void>> pollPolicy) {
		this.poll = new AsyncSupplierWithStatus<>(pollPolicy.apply(this::doPoll));
		return this;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return checkout()
				.whenResult($ -> poll());
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		poll = null;
		return isValid() ?
				sync().whenComplete(($, e) -> invalidateInternalState()) :
				Promise.complete();
	}

	@NotNull
	public Promise<Void> checkout() {
		checkState(commitId == null);
		return repository.checkout()
				.whenResult(checkoutData -> {
					state.init();
					apply(checkoutData.getDiffs());

					workingDiffs.clear();

					commitId = checkoutData.getCommitId();
					level = checkoutData.getLevel();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private boolean isSyncing() {
		return sync.isRunning();
	}

	private boolean isPolling() {
		return poll != null;
	}

	@NotNull
	public Promise<Void> sync() {
		return sync.get();
	}

	@NotNull
	private Promise<Void> doSync() {
		checkState(isValid());
		return sequence(
				this::push,
				poll == null ? this::fetch : Promise::complete,
				this::commit,
				this::push)
				.whenComplete(($, e) -> poll())
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private void poll() {
		if (poll != null && !poll.isRunning()) {
			poll.get()
					.async()
					.whenComplete(($, e) -> {
						if (!sync.isRunning()) {
							poll();
						}
					});
		}
	}

	@NotNull
	private Promise<Void> fetch() {
		K fetchCommitId = this.commitId;
		return repository.fetch(fetchCommitId)
				.whenResult(fetchData -> rebase(fetchCommitId, fetchData))
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@NotNull
	private Promise<Void> doPoll() {
		if (!isValid()) return Promise.complete();
		K pollCommitId = this.commitId;
		return repository.poll(pollCommitId)
				.whenResult(fetchData -> {
					if (!sync.isRunning()) {
						rebase(pollCommitId, fetchData);
					}
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private void rebase(K originalCommitId, FetchData<K, D> fetchData) {
		logger.info("Rebasing - {} {}", originalCommitId, fetchData);
		if (commitId != originalCommitId) return;
		if (pendingCommit != null) return;

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

		commitId = fetchData.getCommitId();
		level = fetchData.getLevel();
	}

	@NotNull
	private Promise<Void> commit() {
		assert pendingCommit == null;
		if (workingDiffs.isEmpty()) return Promise.complete();
		int originalSize = workingDiffs.size();
		List<D> diffs = new ArrayList<>(otSystem.squash(workingDiffs));
		return repository.createCommit(this.commitId, diffs, level)
				.whenResult(commit -> {
					assert pendingCommit == null;
					pendingCommit = commit;
					pendingCommitDiffs = diffs;
					workingDiffs = new ArrayList<>(workingDiffs.subList(originalSize, workingDiffs.size()));
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@NotNull
	private Promise<Void> push() {
		if (pendingCommit == null) return Promise.complete();
		K currentCommitId = this.commitId;
		return repository.push(pendingCommit)
				.whenResult(fetchData -> {
					pendingCommit = null;
					pendingCommitDiffs = null;
					rebase(currentCommitId, fetchData);
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public void reset() {
		checkState(!sync.isRunning());
		apply(otSystem.invert(
				concat(pendingCommitDiffs != null ? pendingCommitDiffs : emptyList(), workingDiffs)));
		workingDiffs.clear();
		pendingCommit = null;
		pendingCommitDiffs = null;
	}

	public void add(@NotNull D diff) {
		checkState(isValid());
		addAll(singletonList(diff));
	}

	public void addAll(@NotNull List<? extends D> diffs) {
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

		commitId = null;
		level = 0;
		workingDiffs = null;

		pendingCommit = null;
		pendingCommitDiffs = null;

		poll = null;
	}

	public K getCommitId() {
		return checkNotNull(commitId, "Internal state has been invalidated");
	}

	public OTState<D> getState() {
		return state;
	}

	public boolean isValid() {
		return commitId != null;
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
				"revision=" + commitId +
				" workingDiffs:" + (workingDiffs != null ? workingDiffs.size() : null) +
				" pendingCommits:" + (pendingCommit != null) +
				"}";
	}
}
