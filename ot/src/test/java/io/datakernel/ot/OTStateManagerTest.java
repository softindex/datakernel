package io.datakernel.ot;

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.ot.utils.OTSourceStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static io.datakernel.ot.utils.OTSourceStub.TestSequence.of;
import static io.datakernel.ot.utils.OTSourceStub.create;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.ot.utils.Utils.createTestOp;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class OTStateManagerTest {
	private Eventloop eventloop;
	private OTSystem<TestOp> system;
	private Comparator<Integer> comparator;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		comparator = Integer::compareTo;
		system = createTestOp();
	}

	@Test
	public void testCommitBeforePushFinished() throws ExecutionException, InterruptedException {
		OTRemote<Integer, TestOp> otSource = new OTRemoteDecorator<Integer, TestOp>(create(of(1, 2), comparator)) {
			@Override
			public CompletionStage<Void> push(List<OTCommit<Integer, TestOp>> otCommits) {
				return super.push(otCommits).thenCompose(aVoid -> scheduledResult(eventloop, 100, aVoid));
			}
		};
		OTStateManager<Integer, TestOp> stateManager = new OTStateManager<>(eventloop, system, otSource, comparator, new TestOpState());

		otSource.push(singletonList(OTCommit.ofRoot(0)));
		eventloop.run();

		final CompletableFuture<Void> startFuture = stateManager.start().toCompletableFuture();
		eventloop.run();
		startFuture.get(); // check without exception

		stateManager.add(add(1));
		stateManager.commit();
		eventloop.run();

		stateManager.push();
		stateManager.add(add(1));
		stateManager.commit();
		eventloop.run();

		stateManager.push();
		eventloop.run();

		final CompletableFuture<Set<Integer>> headsFuture = otSource.getHeads().toCompletableFuture();
		eventloop.run();

		final Set<Integer> heads = headsFuture.get();
		assertEquals(1, heads.size());
		assertEquals(2, heads.iterator().next().intValue());
	}

	private class OTRemoteDecorator<K, D> implements OTRemote<K, D> {
		private final OTSourceStub<K, D> remote;

		private OTRemoteDecorator(OTSourceStub<K, D> remote) {
			this.remote = remote;
		}

		@Override
		public CompletionStage<K> createId() {
			return remote.createId();
		}

		@Override
		public CompletionStage<Void> push(List<OTCommit<K, D>> otCommits) {
			return remote.push(otCommits);
		}

		@Override
		public CompletionStage<Set<K>> getHeads() {
			return remote.getHeads();
		}

		@Override
		public CompletionStage<K> getCheckpoint() {
			return remote.getCheckpoint();
		}

		@Override
		public CompletionStage<OTCommit<K, D>> loadCommit(K revisionId) {
			return remote.loadCommit(revisionId);
		}

		@Override
		public String toString() {
			return remote.toString();
		}
	}

	private static <T> CompletionStage<T> scheduledResult(Eventloop eventloop, long delta, T result) {
		final SettableStage<T> stage = SettableStage.create();
		eventloop.schedule(eventloop.currentTimeMillis() + delta, () -> stage.setResult(result));
		return stage;
	}

}