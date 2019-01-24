package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.exceptions.OTTransformException;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.stream.processor.DatakernelRunner;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.ot.OTCommit.ofCommit;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@RunWith(DatakernelRunner.class)
public class OTStateManagerTest {
	private Eventloop eventloop;
	private OTSystem<TestOp> system;

	@Before
	public void before() {
		eventloop = Eventloop.getCurrentEventloop();
		system = createTestOp();
	}

	@Test
	public void testCommitBeforePushFinished() {
		OTRepository<Integer, TestOp> repositoryStub = OTRepositoryStub.create(asList(2, 3));
		OTRepository<Integer, TestOp> repository = new OTRepositoryDecorator<Integer, TestOp>(repositoryStub) {
			@Override
			public Promise<Void> push(Collection<OTCommit<Integer, TestOp>> otCommits) {
				return super.push(otCommits).thenCompose($ -> scheduledResult(eventloop, 100, null));
			}
		};
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, new TestOpState());

		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		await(stateManager.commit());

		await(stateManager.push());
		stateManager.add(add(1));
		await(stateManager.commit());

		await(stateManager.push());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(1, heads.size());
		assertEquals(3, first(heads).intValue());
	}

	@Test
	public void testPullFullHistory() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		for (int i = 1; i <= 5; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());

		await(stateManager.pull());
		assertEquals(5, testOpState.getValue());
	}

	@Test
	public void testPullAfterFetch() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		for (int i = 1; i <= 10; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
			if (i == 3 || i == 5 || i == 7) {
				stateManager.fetch();
				eventloop.run();
			}
		}

		assertEquals(0, testOpState.getValue());

		await(stateManager.pull());
		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testApplyDiffBeforePull() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		for (int i = 1; i <= 10; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());
		stateManager.add(add(1));
		assertEquals(1, testOpState.getValue());

		await(stateManager.pull());
		assertEquals(11, testOpState.getValue());
	}

	@Test
	public void testTwoFetchAndTwoPullOneAfterAnother() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		for (int i = 1; i <= 20; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
			if (i == 5 || i == 15) {
				await(stateManager.fetch());
			}
			if (i == 10 || i == 20) {
				await(stateManager.pull());
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

		initializeRepository(repository, stateManager);

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> g.add(0, 1, asList(set(0, 10))));

		await(stateManager.fetch());

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 15));
		stateManager.rebase();

		assertEquals(10, testOpState.getValue());
		assertEquals(emptyList(), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving2() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		repository.addGraph(g -> g.add(0, 1, set(0, 15)));
		await(stateManager.fetch());

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		stateManager.rebase();

		assertEquals(10, testOpState.getValue());
		assertEquals(asList(set(15, 10)), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving3() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> g.add(0, 1, set(0, 10)));
		await(stateManager.fetch());

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		stateManager.rebase();

		assertEquals(10, testOpState.getValue());
		assertEquals(emptyList(), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving4() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> g.add(0, 1, add(5)));
		await(stateManager.fetch());

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		stateManager.rebase();

		assertEquals(10, testOpState.getValue());
		assertEquals(asList(set(5, 10)), stateManager.getWorkingDiffs());
	}

	@Test
	public void testRebaseConflictResolving5() throws OTTransformException {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);

		assertEquals(0, testOpState.getValue());

		repository.addGraph(g -> g.add(0, 1, add(10)));
		await(stateManager.fetch());

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		stateManager.rebase();

		assertEquals(15, testOpState.getValue());
		assertEquals(asList(add(5)), stateManager.getWorkingDiffs());
	}

	@Test
	public void testParallelPullsAndPushes() {
		OTRepositoryStub<Integer, TestOp> repository = OTRepositoryStub.create(asList(1, 2, 4, 6));
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		initializeRepository(repository, stateManager);             // rev = 0;
		assertEquals(0, testOpState.getValue());

		stateManager.add(asList(add(1), set(1, 5), add(10)));       // rev = 1
		assertEquals(15, testOpState.getValue());
		stateManager.commitAndPush();

		stateManager.add(asList(add(1), set(16, 20), add(10)));     // rev = 2
		assertEquals(30, testOpState.getValue());
		stateManager.commitAndPush();

		repository.addGraph(builder -> builder.add(2, 3, add(3)));
		stateManager.add(add(10));
		assertEquals(40, testOpState.getValue());

		// pull will do nothing, as push below changed revision while pool in process
		stateManager.pull();

		stateManager.commitAndPush();                               // rev = 4 (2 heads: [3,4])
		eventloop.run();
		assertEquals(40, testOpState.getValue());

		await(algorithms.mergeHeadsAndPush());                        // rev = 6
		await(stateManager.pull());

		assertEquals(43, testOpState.getValue());
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

	private static <T> Promise<T> scheduledResult(Eventloop eventloop, long delta, @Nullable T result) {
		SettablePromise<T> promise = new SettablePromise<>();
		eventloop.delay(delta, () -> promise.set(result));
		return promise;
	}

	private void initializeRepository(OTRepository<Integer, TestOp> repository, OTStateManager<Integer, TestOp> stateManager) {
		await(repository.push(ofRoot(0)), repository.saveSnapshot(0, emptyList()));
		await(stateManager.start());
	}


}
