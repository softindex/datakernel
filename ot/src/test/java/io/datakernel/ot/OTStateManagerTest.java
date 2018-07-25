package io.datakernel.ot;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.exceptions.OTTransformException;
import io.datakernel.ot.utils.OTRemoteStub;
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
		OTRemote<Integer, TestOp> remoteStub = OTRemoteStub.create(asList(2, 3));
		OTRemote<Integer, TestOp> remote = new OTRemoteDecorator<Integer, TestOp>(remoteStub) {
			@Override
			public Stage<Void> push(Collection<OTCommit<Integer, TestOp>> otCommits) {
				return super.push(otCommits).thenCompose($ -> scheduledResult(eventloop, 100, null));
			}
		};
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, new TestOpState());

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
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

		CompletableFuture<Set<Integer>> headsFuture = remote.getHeads().toCompletableFuture();
		eventloop.run();

		Set<Integer> heads = headsFuture.get();
		assertEquals(1, heads.size());
		assertEquals(3, first(heads).intValue());
	}

	@Test
	public void testPullFullHistory() {
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 5; i++) {
			remote.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
		}

		assertEquals(0, testOpState.getValue());

		stateManager.pull();
		eventloop.run();
		assertEquals(5, testOpState.getValue());
	}

	@Test
	public void testPullAfterFetch() {
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 10; i++) {
			remote.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
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
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 10; i++) {
			remote.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
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
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		for (int i = 1; i <= 20; i++) {
			remote.doPush(ofCommit(i, i - 1, asList(add(1)), i + 1L));
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
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();

		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		remote.addGraph(g -> {
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
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		remote.addGraph(g -> {
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
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		remote.addGraph(g -> {
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
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		remote.addGraph(g -> {
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
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		TestOpState testOpState = new TestOpState();
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, remote);
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, testOpState);

		Stages.all(remote.push(ofRoot(0)), remote.saveSnapshot(0, emptyList()))
				.thenCompose($ -> stateManager.start());
		eventloop.run();

		assertEquals(0, testOpState.getValue());

		remote.addGraph(g -> {
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

	private class OTRemoteDecorator<K, D> implements OTRemote<K, D> {
		private final OTRemote<K, D> remote;

		private OTRemoteDecorator(OTRemote<K, D> remote) {
			this.remote = remote;
		}

		@Override
		public Stage<OTCommit<K, D>> createCommit(Map<K, ? extends List<? extends D>> parentDiffs, long level) {
			return remote.createCommit(parentDiffs, level);
		}

		@Override
		public Stage<Void> push(Collection<OTCommit<K, D>> otCommits) {
			return remote.push(otCommits);
		}

		@Override
		public Stage<Set<K>> getHeads() {
			return remote.getHeads();
		}

		@Override
		public Stage<Optional<List<D>>> loadSnapshot(K revisionId) {
			return remote.loadSnapshot(revisionId);
		}

		@Override
		public Stage<OTCommit<K, D>> loadCommit(K revisionId) {
			return remote.loadCommit(revisionId);
		}

		@Override
		public Stage<Void> saveSnapshot(K revisionId, List<D> diffs) {
			return remote.saveSnapshot(revisionId, diffs);
		}

		@Override
		public String toString() {
			return remote.toString();
		}
	}

	private static <T> Stage<T> scheduledResult(Eventloop eventloop, long delta, T result) {
		SettableStage<T> stage = new SettableStage<>();
		eventloop.delay(delta, () -> stage.set(result));
		return stage;
	}

}
