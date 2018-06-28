package io.datakernel.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.OTRemoteStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.ot.utils.Utils;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.util.CollectionUtils.set;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTAlgorithmsTest {
	private static final OTSystem<TestOp> TEST_OP = Utils.createTestOp();

	@Test
	public void testLoadAllChangesFromRootWithSnapshot() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		TestOpState opState = new TestOpState();
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		remote.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(1));
			g.add(2, 3, add(1));
			g.add(3, 4, add(1));
			g.add(4, 5, add(1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, remote);

		remote.saveSnapshot(0, asList(add(10)));
		eventloop.run();

		CompletableFuture<List<TestOp>> changes = remote.getHeads().thenCompose(heads ->
				algorithms.checkout(getLast(heads)))
				.toCompletableFuture();
		eventloop.run();
		changes.get().forEach(opState::apply);

		assertEquals(15, opState.getValue());
	}

	@Test
	public void testReduceEdges() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		remote.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(1));
			g.add(2, 3, add(1));
			g.add(3, 4, add(-1));
			g.add(4, 5, add(-1));
			g.add(3, 6, add(1));
			g.add(6, 7, add(1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, remote);
		CompletableFuture<Map<Integer, List<TestOp>>> future = algorithms.reduceEdges(
				set(5, 7),
				0,
				DiffsReducer.toList())
				.toCompletableFuture();

		eventloop.run();
		Map<Integer, List<TestOp>> result = future.get();

		assertEquals(1, applyToState(result.get(5)));
		assertEquals(5, applyToState(result.get(7)));
	}

	@Test
	public void testReduceEdges2() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		OTRemoteStub<Integer, TestOp> remote = OTRemoteStub.create();
		remote.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(0, 2, add(-1));
			g.add(1, 3, add(1));
			g.add(1, 4, add(-1));
			g.add(2, 4, add(1));
			g.add(2, 5, add(-1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, remote);

		CompletableFuture<Map<Integer, List<TestOp>>> future = algorithms.reduceEdges(
				set(3, 4, 5),
				0,
				DiffsReducer.toList())
				.toCompletableFuture();

		eventloop.run();
		Map<Integer, List<TestOp>> result = future.get();

		assertEquals(2, applyToState(result.get(3)));
		assertEquals(0, applyToState(result.get(4)));
		assertEquals(-2, applyToState(result.get(5)));
	}

	private static int applyToState(List<TestOp> diffs) {
		TestOpState opState = new TestOpState();
		diffs.forEach(opState::apply);
		return opState.getValue();
	}

	private static <T> T getLast(Iterable<T> iterable) {
		Iterator<T> iterator = iterable.iterator();
		while (iterator.hasNext()) {
			T next = iterator.next();
			if (!iterator.hasNext()) return next;
		}
		throw new IllegalArgumentException("Empty iterable");
	}

}