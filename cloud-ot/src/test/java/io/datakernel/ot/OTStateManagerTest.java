package io.datakernel.ot;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.stream.processor.DatakernelRunner;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.ot.OTCommit.ofCommit;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@RunWith(DatakernelRunner.class)
public class OTStateManagerTest {
	private static final StacklessException FAILED = new StacklessException("Failed");

	private OTRepositoryStub<Integer, TestOp> repository;
	private OTAlgorithms<Integer, TestOp> algorithms;
	private OTStateManager<Integer, TestOp> stateManager;
	private TestOpState testOpState;
	private boolean alreadyFailed = false;

	@Before
	public void before() {
		Random random = new Random();
		repository = OTRepositoryStub.create();
		repository.revisionIdSupplier = () -> random.nextInt(1000) + 1000;
		algorithms = new OTAlgorithms<>(getCurrentEventloop(), createTestOp(), repository);
		testOpState = new TestOpState();
		stateManager = new OTStateManager<>(getCurrentEventloop(), algorithms.getOtSystem(), algorithms.getOtNode(), testOpState);

		initializeRepository(repository, stateManager);
	}

	@Test
	public void testSyncBeforeSyncFinished() {
		repository.revisionIdSupplier = () -> 2;
		OTNode<Integer, TestOp> otNode = new OTNodeDecorator(algorithms.getOtNode()) {
			@Override
			public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
				return super.fetch(currentCommitId).thenCompose(fetchData -> scheduledResult(getCurrentEventloop(), 100, fetchData));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(getCurrentEventloop(), algorithms.getOtSystem(), otNode, testOpState);

		initializeRepository(repository, stateManager);
		stateManager.add(add(1));
		stateManager.sync();
		await(stateManager.sync());

		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());
		assertEquals((Integer) 2, stateManager.getRevision());
		assertEquals(1, testOpState.getValue());
	}

	@Test
	public void testSyncFullHistory() {
		for (int i = 1; i <= 5; i++) {
			repository.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());

		await(stateManager.sync());
		assertEquals(5, testOpState.getValue());
	}

	@Test
	public void testApplyDiffBeforeSync() {
		repository.revisionIdSupplier = () -> 11;
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
	public void testMultipleSyncs() {
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
	public void testSyncAfterFailedCommit() {
		repository.revisionIdSupplier = () -> 1;
		OTNode<Integer, TestOp> otNode = new OTNodeDecorator(algorithms.getOtNode()) {
			@Override
			public Promise<OTCommit<Integer, TestOp>> createCommit(Integer parent, List<? extends TestOp> diffs, long level) {
				return failOnce(() -> super.createCommit(parent, diffs, level));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(getCurrentEventloop(), algorithms.getOtSystem(), otNode, testOpState);
		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		Throwable exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getRevision());
		assertFalse(stateManager.hasPendingCommits());
		assertTrue(stateManager.hasWorkingDiffs());

		// new ops added in the meantime
		stateManager.add(add(100));

		await(stateManager.sync());
		assertEquals((Integer) 1, stateManager.getRevision());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(singleton(1), heads);
		assertEquals(101, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedPull() {
		repository.revisionIdSupplier = () -> 3;
		OTNode<Integer, TestOp> otNode = new OTNodeDecorator(algorithms.getOtNode()) {
			@Override
			public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
				return failOnce(() -> super.fetch(currentCommitId));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(getCurrentEventloop(), algorithms.getOtSystem(), otNode, testOpState);
		initializeRepository(repository, stateManager);
		repository.setGraph(g -> {
			g.add(0, 1, add(10));
			g.add(1, 2, add(20));
		});

		stateManager.add(add(1));
		Throwable exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getRevision());
		assertFalse(stateManager.hasPendingCommits());
		assertTrue(stateManager.hasWorkingDiffs());

		// new ops added in the meantime
		stateManager.add(add(100));

		await(stateManager.sync());
		assertEquals((Integer) 3, stateManager.getRevision());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(singleton(3), heads);
		assertEquals(131, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedPush() {
		repository.revisionIdSupplier = asList(3, 4, 5).iterator()::next;
		OTNode<Integer, TestOp> otNode = new OTNodeDecorator(algorithms.getOtNode()) {
			@Override
			public Promise<Void> push(OTCommit<Integer, TestOp> commit) {
				return failOnce(() -> super.push(commit));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(getCurrentEventloop(), algorithms.getOtSystem(), otNode, testOpState);
		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		Throwable exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 3, stateManager.getRevision());
		assertTrue(stateManager.hasPendingCommits());
		assertFalse(stateManager.hasWorkingDiffs());

		// new ops added in the meantime, repo changed
		stateManager.add(add(100));
		repository.setGraph(g -> {
			g.add(0, 1, add(10));
			g.add(1, 2, add(20));

		});

		await(stateManager.sync());
		assertEquals((Integer) 5, stateManager.getRevision());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(singleton(5), heads);
		assertEquals(131, testOpState.getValue());
	}

	class OTNodeDecorator implements OTNode<Integer, TestOp> {
		private final OTNode<Integer, TestOp> node;

		OTNodeDecorator(OTNode<Integer, TestOp> node) {
			this.node = node;
		}

		@Override
		public Promise<OTCommit<Integer, TestOp>> createCommit(Integer parent, List<? extends TestOp> diffs, long level) {
			return node.createCommit(parent, diffs, level);
		}

		@Override
		public Promise<Void> push(OTCommit<Integer, TestOp> commit) {
			return node.push(commit);
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> checkout() {
			return node.checkout();
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
			return node.fetch(currentCommitId);
		}
	}

	private <T> Promise<T> failOnce(AsyncSupplier<T> supplier) {
		if (alreadyFailed) {
			return supplier.get();
		} else {
			alreadyFailed = true;
			return Promise.ofException(FAILED);
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
