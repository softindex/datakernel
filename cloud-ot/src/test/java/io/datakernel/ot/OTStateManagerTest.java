package io.datakernel.ot;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.ot.OTCommit.ofCommit;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTStateManagerTest {
	private static final StacklessException FAILED = new StacklessException("Failed");
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private OTRepositoryStub<Integer, TestOp> repository;
	private OTUplinkImpl<Integer, TestOp, OTCommit<Integer, TestOp>> uplink;
	private OTStateManager<Integer, TestOp> stateManager;
	private TestOpState testOpState;
	private boolean alreadyFailed = false;

	@Before
	public void before() {
		Random random = new Random();
		repository = OTRepositoryStub.create();
		repository.revisionIdSupplier = () -> random.nextInt(1000) + 1000;
		testOpState = new TestOpState();
		uplink = OTUplinkImpl.create(this.repository, SYSTEM);
		stateManager = OTStateManager.create(getCurrentEventloop(), SYSTEM, uplink, testOpState);

		initializeRepository(this.repository, stateManager);
	}

	@Test
	public void testSyncBeforeSyncFinished() {
		repository.revisionIdSupplier = () -> 2;
		OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
				return super.fetch(currentCommitId).then(fetchData -> {
					getCurrentEventloop();
					return Promises.delay(100L, fetchData);
				});
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentEventloop(), SYSTEM, uplink, testOpState);

		initializeRepository(repository, stateManager);
		stateManager.add(add(1));
		stateManager.sync();
		await(stateManager.sync());

		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());
		assertEquals((Integer) 2, stateManager.getCommitId());
		assertEquals(1, testOpState.getValue());
	}

	@Test
	public void testSyncFullHistory() {
		for (int i = 1; i <= 5; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());

		await(stateManager.sync());
		assertEquals(5, testOpState.getValue());
	}

	@Test
	public void testApplyDiffBeforeSync() {
		repository.revisionIdSupplier = () -> 11;
		for (int i = 1; i <= 10; i++) {
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, asList(add(1)), i + 1L));
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
			repository.doPushAndUpdateHead(ofCommit(0, i, i - 1, asList(add(1)), i + 1L));
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
	public void testMultibinders() {
		repository.addGraph(g -> g.add(0, 1, asList(add(5))));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(3));
		await(stateManager.sync());

		assertEquals(8, testOpState.getValue());
	}

	@Test
	public void testMultibinders2() {
		repository.addGraph(g -> g.add(0, 1, set(0, 15)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testMultibinders3() {
		repository.addGraph(g -> g.add(0, 1, set(0, 10)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testMultibinders4() {
		repository.addGraph(g -> g.add(0, 1, add(5)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(set(0, 10));
		await(stateManager.sync());

		assertEquals(10, testOpState.getValue());
	}

	@Test
	public void testMultibinders5() {
		repository.addGraph(g -> g.add(0, 1, add(10)));

		assertEquals(0, testOpState.getValue());

		stateManager.add(add(5));
		await(stateManager.sync());

		assertEquals(15, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedCommit() {
		repository.revisionIdSupplier = () -> 1;
		OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<OTCommit<Integer, TestOp>> createProtoCommit(Integer parent, List<TestOp> diffs, long parentLevel) {
				return failOnce(() -> super.createProtoCommit(parent, diffs, parentLevel));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentEventloop(), SYSTEM, uplink, testOpState);
		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		Throwable exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getCommitId());
		assertFalse(stateManager.hasPendingCommits());
		assertTrue(stateManager.hasWorkingDiffs());

		// new ops added in the meantime
		stateManager.add(add(100));

		await(stateManager.sync());
		assertEquals((Integer) 1, stateManager.getCommitId());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(singleton(1), heads);
		assertEquals(101, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedPull() {
		repository.revisionIdSupplier = () -> 3;
		OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
				return failOnce(() -> super.fetch(currentCommitId));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentEventloop(), SYSTEM, uplink, testOpState);
		initializeRepository(repository, stateManager);
		repository.setGraph(g -> {
			g.add(0, 1, add(10));
			g.add(1, 2, add(20));
		});

		stateManager.add(add(1));
		Throwable exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getCommitId());
		assertFalse(stateManager.hasPendingCommits());
		assertTrue(stateManager.hasWorkingDiffs());

		// new ops added in the meantime
		stateManager.add(add(100));

		await(stateManager.sync());
		assertEquals((Integer) 3, stateManager.getCommitId());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(singleton(3), heads);
		assertEquals(131, testOpState.getValue());
	}

	@Test
	public void testSyncAfterFailedPush() {
		repository.revisionIdSupplier = asList(3, 4, 5).iterator()::next;
		OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> uplink = new OTUplinkDecorator(OTStateManagerTest.this.uplink) {
			@Override
			public Promise<FetchData<Integer, TestOp>> push(OTCommit<Integer, TestOp> protoCommit) {
				return failOnce(() -> super.push(protoCommit));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = OTStateManager.create(getCurrentEventloop(), SYSTEM, uplink, testOpState);
		initializeRepository(repository, stateManager);

		stateManager.add(add(1));
		Throwable exception = awaitException(stateManager.sync());

		assertEquals(FAILED, exception);
		assertEquals((Integer) 0, stateManager.getCommitId());
		assertTrue(stateManager.hasPendingCommits());
		assertFalse(stateManager.hasWorkingDiffs());

		// new ops added in the meantime, repo changed
		stateManager.add(add(100));
		repository.setGraph(g -> {
			g.add(0, 1, add(10));
			g.add(1, 2, add(20));

		});

		await(stateManager.sync());
		assertEquals((Integer) 5, stateManager.getCommitId());
		assertFalse(stateManager.hasWorkingDiffs());
		assertFalse(stateManager.hasPendingCommits());

		Set<Integer> heads = await(repository.getHeads());
		assertEquals(singleton(5), heads);
		assertEquals(131, testOpState.getValue());
	}

	class OTUplinkDecorator implements OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> {
		private final OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> node;

		OTUplinkDecorator(OTUplink<Integer, TestOp, OTCommit<Integer, TestOp>> node) {
			this.node = node;
		}

		@Override
		public Promise<OTCommit<Integer, TestOp>> createProtoCommit(Integer parent, List<TestOp> diffs, long parentLevel) {
			return node.createProtoCommit(parent, diffs, parentLevel);
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> push(OTCommit<Integer, TestOp> protoCommit) {
			return node.push(protoCommit);
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> checkout() {
			return node.checkout();
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> fetch(Integer currentCommitId) {
			return node.fetch(currentCommitId);
		}

		@Override
		public Promise<FetchData<Integer, TestOp>> poll(Integer currentCommitId) {
			return node.poll(currentCommitId);
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

	private void initializeRepository(OTRepository<Integer, TestOp> repository, OTStateManager<Integer, TestOp> stateManager) {
		await(repository.pushAndUpdateHead(ofRoot(0)), repository.saveSnapshot(0, emptyList()));
		await(stateManager.checkout());
	}

}
