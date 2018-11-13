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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.ot.exceptions.OTTransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Preconditions.*;
import static io.datakernel.util.Utils.coalesce;
import static java.util.Collections.*;

public final class OTStateManager<K, D> implements EventloopService, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;

	private final OTAlgorithms<K, D> algorithms;
	private final OTSystem<D> otSystem;
	private final OTRepository<K, D> repository;
	private Supplier<Promise<Void>> checkoutValidator;

	private OTState<D> state;

	@Nullable
	private K revision;
	private Long revisionLevel;
	private List<D> workingDiffs = new ArrayList<>();

	@Nullable
	private K fetchedRevision;
	@Nullable
	private Long fetchedRevisionLevel;
	@Nullable
	private List<D> fetchedDiffs;

	private Map<K, OTCommit<K, D>> pendingCommits = new HashMap<>();

	OTStateManager(Eventloop eventloop, OTAlgorithms<K, D> algorithms, OTState<D> state) {
		this.eventloop = eventloop;
		this.algorithms = algorithms;
		this.otSystem = algorithms.getOtSystem();
		this.repository = algorithms.getRepository();
		this.state = state;
	}

	public static <K, D> OTStateManager<K, D> create(Eventloop eventloop, OTAlgorithms<K, D> algorithms, OTState<D> state) {
		checkArgument(eventloop != null, "Cannot create OTStateManager with Eventloop that is null");
		checkArgument(algorithms != null, "Cannot create OTStateManager with OTAlgorithms that is null");
		checkArgument(state != null, "Cannot create OTStateManager with OTState that is null");
		return new OTStateManager<>(eventloop, algorithms, state);
	}

	private static <D> List<D> concatLists(List<D> a, List<D> b) {
		return concat(a, b);
	}

	public OTStateManager<K, D> withCheckoutValidator(Supplier<Promise<Void>> stateValidator) {
		this.checkoutValidator = stateValidator;
		return this;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<Void> start() {
		return checkout().toVoid();
	}

	@Override
	public Promise<Void> stop() {
		invalidateInternalState();
		return Promise.complete();
	}

	public Promise<K> checkout() {
		return repository.getHeads()
				.thenCompose(heads -> checkout(first(heads)))
				.thenCompose($ -> pull())
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public Promise<K> checkout(K commitId) {
		return repository.loadCommit(commitId)
				.thenCompose(commit -> algorithms.checkout(commitId)
						.thenApply(diffs -> {
							state.init();
							apply(diffs);

							workingDiffs.clear();
							pendingCommits.clear();

							revision = commit.getId();
							revisionLevel = commit.getLevel();

							fetchedRevision = null;
							fetchedRevisionLevel = null;
							fetchedDiffs = null;

							return revision;
						}))
				.thenCompose(k -> {
					if (checkoutValidator == null) return Promise.of(k);
					return checkoutValidator.get()
							.whenException(e -> invalidateInternalState())
							.thenApply($ -> k);
				})
				.whenComplete(toLogger(logger, thisMethod(), commitId, this));
	}

	private final AsyncSupplier<K> fetch = reuse(this::doFetch);

	public Promise<K> fetch() {
		return fetch.get();
	}

	public Promise<K> fetch(K head) {
		fetchedRevision = null;
		fetchedRevisionLevel = null;
		fetchedDiffs = null;
		return doFetch(singleton(head));
	}

	private Promise<K> doFetch() {
		if (!pendingCommits.isEmpty()) return Promise.of(null);
		return repository.getHeads()
				.thenCompose(this::doFetch);
	}

	private Promise<K> doFetch(Set<K> heads) {
		if (!pendingCommits.isEmpty()) return Promise.of(null);
		K fetchedRevisionCopy = coalesce(fetchedRevision, revision);
		return algorithms.findParent(heads,
				DiffsReducer.toList(),
				commit -> Promise.of(commit.getId().equals(fetchedRevisionCopy)))
				.thenCompose(findResult -> {
					if (!findResult.isFound()) {
						logger.warn("Commit not found: {} in {}", heads, this);
						return Promise.of(null);
					}

					if (fetchedRevisionCopy != coalesce(fetchedRevision, revision)) {
						logger.info("Concurrent revision modification: {} in {}", fetchedRevisionCopy, this);
						return Promise.of(null);
					}

					fetchedRevision = findResult.getChild();
					fetchedRevisionLevel = findResult.getChildLevel();
					fetchedDiffs = otSystem.squash(concat(
							coalesce(fetchedDiffs, emptyList()),
							findResult.getAccumulatedDiffs()));

					return Promise.of(fetchedRevision);
				})
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public Promise<K> pull() {
		return fetch()
				.thenCompose(this::doPull)
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public Promise<K> pull(K pullRevision) {
		return fetch(pullRevision)
				.thenCompose(this::doPull)
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private Promise<K> doPull(K fetchedRevision) {
		if (fetchedRevision == null) return Promise.of(null);
		try {
			return Promise.of(rebase());
		} catch (OTTransformException e) {
			invalidateInternalState();
			return Promise.ofException(e);
		}
	}

	public K rebase() throws OTTransformException {
		if (fetchedRevision == null) return null;

		TransformResult<D> transformed = otSystem.transform(
				otSystem.squash(workingDiffs),
				otSystem.squash(fetchedDiffs));
		apply(transformed.left);
		workingDiffs = new ArrayList<>(transformed.right);

		revision = checkNotNull(fetchedRevision);
		revisionLevel = checkNotNull(fetchedRevisionLevel);

		fetchedRevision = null;
		fetchedRevisionLevel = null;
		fetchedDiffs = null;

		return revision;
	}

	public void reset() {
		List<D> diffs = new ArrayList<>(workingDiffs);
		diffs = otSystem.invert(diffs);
		apply(diffs);
		workingDiffs = new ArrayList<>();
	}

	public Promise<K> commitAndPush() {
		return commit()
				.thenCompose(id -> push().thenApply($ -> id))
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncSupplier<K> commit = reuse(this::doCommit);

	public Promise<K> commit() {
		return commit.get();
	}

	Promise<K> doCommit() {
		if (workingDiffs.isEmpty()) {
			return Promise.of(null);
		}
		K revisionCopy = revision;
		List<D> workingDiffsCopy = new ArrayList<>(workingDiffs);
		return repository.createCommit(revision, otSystem.squash(workingDiffs), revisionLevel + 1L)
				.thenApply(newCommit -> {
					if (revisionCopy != revision || !isShallowEquals(workingDiffs, workingDiffsCopy)) {
						return null;
					}
					pendingCommits.put(newCommit.getId(), newCommit);

					revision = newCommit.getId();
					revisionLevel = newCommit.getLevel();
					workingDiffs = new ArrayList<>();

					fetchedRevision = null;
					fetchedRevisionLevel = null;
					fetchedDiffs = null;

					return newCommit.getId();
				})
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private final AsyncSupplier<Void> push = reuse(this::doPush);

	public Promise<Void> push() {
		return push.get();
	}

	Promise<Void> doPush() {
		List<OTCommit<K, D>> list = new ArrayList<>(pendingCommits.values());
		return repository.push(list)
				.whenResult($ -> list.stream().map(OTCommit::getId).forEach(pendingCommits::remove))
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public void add(D diff) {
		add(singletonList(diff));
	}

	public void add(List<D> diffs) {
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
		revisionLevel = null;
		workingDiffs = null;

		fetchedRevision = null;
		fetchedRevisionLevel = null;
		fetchedDiffs = null;

		pendingCommits = null;
	}

	private boolean isInternalStateValid() {
		checkState(state == null || (revision != null && pendingCommits != null));
		checkState(revision == null || (revisionLevel != null && workingDiffs != null));
		checkState(fetchedRevision == null || (fetchedRevisionLevel != null && fetchedDiffs != null));
		return state != null;
	}

	public OTAlgorithms<K, D> getAlgorithms() {
		return algorithms;
	}

	public OTState<D> getState() {
		return checkNotNull(state);
	}

	public K getRevision() {
		return checkNotNull(revision);
	}

	List<D> getWorkingDiffs() {
		return workingDiffs;
	}

	public boolean hasWorkingDiffs() {
		return !workingDiffs.isEmpty();
	}

	public boolean hasPendingCommits() {
		return !pendingCommits.isEmpty();
	}

	public boolean hasFetchedDiffs() {
		return fetchedDiffs != null;
	}

	@Nullable
	@JmxAttribute(name = "revision")
	public String getJmxRevision() {
		return revision != null ? revision.toString() : null;
	}

	@JmxAttribute
	public String getFetchedRevision() {
		return fetchedRevision != null ? fetchedRevision.toString() : null;
	}

	@JmxAttribute
	public int getFetchedDiffsSize() {
		return fetchedDiffs != null ? fetchedDiffs.size() : -1;
	}

	@JmxAttribute
	public int getPendingCommitsSize() {
		return pendingCommits.size();
	}

	@JmxAttribute
	public int getWorkingCommitsSize() {
		return workingDiffs.size();
	}

	@Override
	public String toString() {
		return "{" +
				"revision=" + revision +
				" workingDiffs:" + (workingDiffs != null ? workingDiffs.size() : null) +
				" fetchedRevision=" + fetchedRevision +
				" fetchedDiffs:" + (fetchedDiffs != null ? fetchedDiffs.size() : null) +
				" pendingCommits:" + (pendingCommits != null ? pendingCommits.size() : null) +
				'}';
	}
}
