package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
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
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@RunWith(DatakernelRunner.class)
public class OTStateManagerTest {
	private Eventloop eventloop;
	private OTSystem<TestOp> system;
	private OTRepositoryStub<Integer, TestOp> repository;
	private OTStateManager<Integer, TestOp> stateManager;
	private TestOpState testOpState;

	@Before
	public void before() {
		eventloop = Eventloop.getCurrentEventloop();
		system = createTestOp();
		repository = OTRepositoryStub.create();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		testOpState = new TestOpState();
		stateManager = new OTStateManager<>(algorithms.getOtSystem(), algorithms.getOtNode(), testOpState);

		initializeRepository(repository, stateManager);
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
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(algorithms.getOtSystem(), algorithms.getOtNode(), testOpState);

		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		await(stateManager.sync());

		stateManager.add(add(1));
		await(stateManager.sync());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(1, heads.size());
		assertEquals(3, first(heads).intValue());
	}

	@Test
	public void testPullFullHistory() {
		for (int i = 1; i <= 5; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());

		await(stateManager.sync());
		assertEquals(5, testOpState.getValue());
	}

	@Test
	public void testPull() {
		for (int i = 1; i <= 10; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
			if (i == 3 || i == 5 || i == 7) {
				await(stateManager.sync());
			}
		}

		assertEquals(7, testOpState.getValue());

		await(stateManager.sync());
		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testApplyDiffBeforePull() {
		for (int i = 1; i <= 10; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());
		stateManager.add(add(1));
		assertEquals(1, testOpState.getValue());

		await(stateManager.sync());
		assertEquals(11, testOpState.getValue());
	}

	@Test
	public void testMultiplePulls() {
		for (int i = 1; i <= 20; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
			if (i == 5 || i == 15) {
				await(stateManager.sync());
			}
			if (i == 10 || i == 20) {
				await(stateManager.sync());
			}
		}

		assertEquals(20, testOpState.getValue());
	}

	@Test
	public void testConflictResolving() {
		repository.addGraph(g -> g.add(0, 1, asList(add(5))));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(3));
		await(stateManager.sync());

		assertEquals(8, testOpState.getValue());
	}

	@Test
	public void testConflictResolving2() {
		repository.addGraph(g -> g.add(0, 1, set(0, 15)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testConflictResolving3() {
		repository.addGraph(g -> g.add(0, 1, set(0, 10)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testConflictResolving4() {
		repository.addGraph(g -> g.add(0, 1, add(5)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testConflictResolving5() {
		repository.addGraph(g -> g.add(0, 1, add(10)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		await(stateManager.sync());

		assertEquals(15, testOpState.getValue());
	}

	@Test
	public void testPullAfterPush() {
		Random random = new Random();
		repository.revisionIdSupplier = random::nextInt;

		Integer initialRevision = stateManager.getRevision();
		stateManager.add(add(3));
		assertEquals(3, testOpState.getValue());
		assertEquals(initialRevision, stateManager.getRevision());

		await(stateManager.sync());
		assertEquals(3, testOpState.getValue());
		Integer afterCommitRevision = stateManager.getRevision();
		assertNotEquals(initialRevision, afterCommitRevision);

		await(stateManager.sync());
		assertEquals(3, testOpState.getValue());
		assertEquals(afterCommitRevision, stateManager.getRevision());

		await(stateManager.sync());
		// Nothing changed
		assertEquals(3, testOpState.getValue());
		assertEquals(afterCommitRevision, stateManager.getRevision());
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
		await(stateManager.checkout());
	}

}
