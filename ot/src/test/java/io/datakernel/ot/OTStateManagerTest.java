package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.exceptions.OTTransformException;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.ot.OTCommit.ofCommit;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTStateManagerTest {
	private Eventloop eventloop;
	private OTSystem<TestOp> system;
	private Comparator<Integer> comparator;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		comparator = Integer::compareTo;
		system = createTestOp();
	}

	@Test
	public void testCommitBeforePushFinished() throws ExecutionException, InterruptedException {
		OTRepository<Integer, TestOp> repositoryStub = OTRepositoryStub.create(asList(2, 3));
		OTRepository<Integer, TestOp> repository = new OTRepositoryDecorator<Integer, TestOp>(repositoryStub) {
			@Override
			public Promise<Void> push(Collection<OTCommit<Integer, TestOp>> otCommits) {
				return super.push(otCommits).thenCompose($ -> scheduledResult(eventloop, 100, null));
			}
		};
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, new TestOpState());

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		stateManager.add(add(1));
		stateManager.commit();
		eventloop.run();

		stateManager.push();
		stateManager.add(add(1));
		stateManager.commit();
		eventloop.run();

		stateManager.push();
		eventloop.run();

		CompletableFuture<Set<Integer>> headsFuture = repository.getHeads().toCompletableFuture();
		eventloop.run();

		Set<Integer> heads = headsFuture.get();
		assertEquals(1, heads.size());
		assertEquals(3, first(heads).intValue());
	}

	@Test
	public void testPullFullHistory() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 5; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());

		stateManager.pull();
		eventloop.run();
		assertEquals(5, testOpState.getValue());
	}

	@Test
	public void testPullAfterFetch() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 10; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
			if (i == 3 || i == 5 || i == 7) {
				stateManager.fetch();
				eventloop.run();
			}
		}

		assertEquals(0, testOpState.getValue());

		stateManager.pull();
		eventloop.run();
		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testApplyDiffBeforePull() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 10; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());
		stateManager.add(add(1));
		eventloop.run();
		assertEquals(1, testOpState.getValue());

		stateManager.pull();
		eventloop.run();
		assertEquals(11, testOpState.getValue());
	}

	@Test
	public void testTwoFetchAndTwoPullOneAfterAnother() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 20; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
			if (i == 5 || i == 15) {
				stateManager.fetch();
				eventloop.run();
			}
			if (i == 10 || i == 20) {
				stateManager.pull();
				eventloop.run();
			}
		}

		assertEquals(20, testOpState.getValue());
	}

	@Test
	public void testRebaseConflictResolving() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();

		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> {
			g.add(0, 1, asList(set(0, 10)));
		});

		stateManager.fetch();
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 15));
		stateManager.rebase();
		eventloop.run();

		assertEquals(10, testOpState.getValue());
		assertEquals(emptyList(), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving2() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		repository.addGraph(g -> {
			g.add(0, 1, set(0, 15));
		});
		stateManager.fetch();
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		stateManager.rebase();

		eventloop.run();
		assertEquals(10, testOpState.getValue());
		assertEquals(asList(set(15, 10)), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving3() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> {
			g.add(0, 1, set(0, 10));
		});
		stateManager.fetch();
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		stateManager.rebase();

		eventloop.run();
		assertEquals(10, testOpState.getValue());
		assertEquals(emptyList(), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving4() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> {
			g.add(0, 1, add(5));
		});
		stateManager.fetch();
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		stateManager.rebase();

		eventloop.run();
		assertEquals(10, testOpState.getValue());
		assertEquals(asList(set(5, 10)), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving5() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Promises.all(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> {
			g.add(0, 1, add(10));
		});
		stateManager.fetch();
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		stateManager.rebase();

		eventloop.run();
		assertEquals(15, testOpState.getValue());
		assertEquals(asList(add(5)), stateManager.getWorkingDiffs());
	}

	private class OTRepositoryDecorator<K, D> implements OTRepository<K, D> {
		private final OTRepository<K, D> repository;

		private OTRepositoryDecorator(OTRepository<K, D> repository) {
			this.repository = repository;
		}

		@Override
		public Promise<OTCommit<K, D>> createCommit(Map<K, ? extends List<? extends D>> parentDiffs, long level) {
			return repository.createCommit(parentDiffs, level);
		}

		@Override
		public Promise<Void> push(Collection<OTCommit<K, D>> otCommits) {
			return repository.push(otCommits);
		}

		@Override
		public Promise<Set<K>> getHeads() {
			return repository.getHeads();
		}

		@Override
		public Promise<Optional<List<D>>> loadSnapshot(K revisionId) {
			return repository.loadSnapshot(revisionId);
		}

		@Override
		public Promise<OTCommit<K, D>> loadCommit(K revisionId) {
			return repository.loadCommit(revisionId);
		}

		@Override
		public Promise<Void> saveSnapshot(K revisionId, List<D> diffs) {
			return repository.saveSnapshot(revisionId, diffs);
		}

		@Override
		public String toString() {
			return repository.toString();
		}
	}

	private static <T> Promise<T> scheduledResult(Eventloop eventloop, long delta, T result) {
		SettablePromise<T> promise = new SettablePromise<>();
		eventloop.delay(delta, () -> promise.set(result));
		return promise;
	}

}
