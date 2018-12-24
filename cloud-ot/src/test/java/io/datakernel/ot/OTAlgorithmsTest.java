package io.datakernel.ot;

import io.datakernel.async.Promises;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.ot.utils.Utils;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.util.CollectionUtils.getLast;
import static io.datakernel.util.CollectionUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTAlgorithmsTest {
	private static final OTSystem<TestOp> TEST_OP = Utils.createTestOp();
	private static final OTRepositoryStub<Integer, TestOp> REPOSITORY = OTRepositoryStub.create();
	private static Eventloop eventloop;
	private static OTAlgorithms<Integer, TestOp> algorithms;

	@Before
	public void reset() {
		eventloop = Eventloop.getCurrentEventloop();
		algorithms = new OTAlgorithms<>(eventloop, TEST_OP, REPOSITORY);
		REPOSITORY.reset();
	}

	@Test
	public void testLoadAllChangesFromRootWithSnapshot() throws ExecutionException, InterruptedException {
		TestOpState opState = new TestOpState();
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(1));
			g.add(2, 3, add(1));
			g.add(3, 4, add(1));
			g.add(4, 5, add(1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, REPOSITORY);

		REPOSITORY.saveSnapshot(0, asList(add(10)));
		eventloop.run();

		CompletableFuture<List<TestOp>> changes = REPOSITORY.getHeads().thenCompose(heads ->
				algorithms.checkout(getLast(heads)))
				.toCompletableFuture();
		eventloop.run();
		changes.get().forEach(opState::apply);

		assertEquals(15, opState.getValue());
	}

	@Test
	public void testReduceEdges() throws ExecutionException, InterruptedException {
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(1));
			g.add(2, 3, add(1));
			g.add(3, 4, add(-1));
			g.add(4, 5, add(-1));
			g.add(3, 6, add(1));
			g.add(6, 7, add(1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, REPOSITORY);
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
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(0, 2, add(-1));
			g.add(1, 3, add(1));
			g.add(1, 4, add(-1));
			g.add(2, 4, add(1));
			g.add(2, 5, add(-1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, REPOSITORY);

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

	@Test
	public void testCheckoutSnapshotInAnotherBranch() {
		graph1();
		doTestCheckoutGraph1(5, singletonList(add(6)));
	}

	@Test
	public void testCheckoutSnapshotInSameBranch() {
		graph1();
		doTestCheckoutGraph1(7, singletonList(add(8)));
	}

	@Test
	public void testCheckoutSnapshotInCommonBranch() {
		graph1();
		doTestCheckoutGraph1(2, singletonList(add(2)));
	}

	@Test
	public void testCheckoutSnapshotIsRoot() {
		graph1();
		doTestCheckoutGraph1(0, singletonList(add(0)));
		graph2();
		doTestCheckoutGraph2(0, singletonList(add(0)));
	}

	@Test
	public void testCheckoutSnapshotIsCheckoutCommit() {
		graph1();
		doTestCheckoutGraph1(9, singletonList(add(16)));
		graph2();
		doTestCheckoutGraph2(2, singletonList(add(2)));
	}

	@Test
	public void testDiffBetween() {
		graph1();
		algorithms.diff(5, 9)
				.whenComplete(assertComplete(diff -> assertEquals(applyToState(asList(add(10))), applyToState(diff)))); // -2, +4, +4, +4
		eventloop.run();

		algorithms.diff(5, 0)
				.whenComplete(assertComplete(diff -> assertEquals(applyToState(asList(add(-6))), applyToState(diff)))); // -2, -1, -1, -1, -1
		eventloop.run();

		algorithms.diff(5, 6)
				.whenComplete(assertComplete(diff -> assertEquals(applyToState(asList(add(+3))), applyToState(diff)))); // +3
		eventloop.run();

		algorithms.diff(5, 5)
				.whenComplete(assertComplete(diff -> assertEquals(emptyList(), diff))); // 0
		eventloop.run();

		graph2();
		algorithms.diff(6, 3)
				.whenComplete(assertComplete(diff -> assertEquals(applyToState(asList(add(-2))), applyToState(diff)))); // -1, -1
		eventloop.run();

		algorithms.diff(0, 6)
				.whenComplete(assertComplete(diff -> assertEquals(applyToState(asList(add(5))), applyToState(diff)))); // +1, +1, +1, +1, +1
		eventloop.run();

		algorithms.diff(4, 5)
				.whenComplete(assertComplete(diff -> assertEquals(applyToState(emptyList()), applyToState(diff)))); // 0
		eventloop.run();

		algorithms.diff(3, 2)
				.whenComplete(assertComplete(diff -> assertEquals(applyToState(asList(add(-1))), applyToState(diff)))); // -1
		eventloop.run();

	}

	private void doTestCheckoutGraph1(int snaphotId, List<TestOp> snapshotDiffs) {
		doTestCheckout(snaphotId, snapshotDiffs);

		algorithms.checkout(9)
				.whenComplete(assertComplete(diffs -> assertEquals(16, applyToState(diffs))));
		eventloop.run();
	}

	private void doTestCheckoutGraph2(int snaphotId, List<TestOp> snapshotDiffs) {
		doTestCheckout(snaphotId, snapshotDiffs);

		eventloop.run();

		algorithms.checkout(6)
				.whenComplete(assertComplete(diffs -> assertEquals(5, applyToState(diffs))));
		eventloop.run();
	}


	private void doTestCheckout(int snaphotId, List<TestOp> snapshotDiffs) {
		// pushing root commit + snapshot
		Promises.all(REPOSITORY.push(ofRoot(0)));
		eventloop.run();

		// saving snapshot on
		REPOSITORY.saveSnapshot(snaphotId, snapshotDiffs);
		eventloop.run();
	}

	private static void graph1() {
		REPOSITORY.reset();
		/*
		digraph G {
			"1" -> "0" [dir=back label = "+1"]
			"2" -> "1" [dir=back label = "+1"]
			"3" -> "2" [dir=back label = "+1"]
			"4" -> "3" [dir=back label = "+1"]
			"5" -> "4" [dir=back label = "+2"]
			"6" -> "5" [dir=back label = "+3"]

			"7" -> "4" [dir=back label = "+4"]
			"8" -> "7" [dir=back label = "+4"]
			"9" -> "8" [dir=back label = "+4"]

			"0" [label="Root" style=filled fillcolor=green]
		}
		 */
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(1));
			g.add(2, 3, add(1));
			g.add(3, 4, add(1));

			// branch 1
			g.add(4, 5, add(2));
			g.add(5, 6, add(3));

			//branch 2
			g.add(4, 7, add(4));
			g.add(7, 8, add(4));
			g.add(8, 9, add(4));
		});
	}

	private static void graph2() {
		REPOSITORY.reset();
		/*
		digraph G {
			"1" -> "0" [dir=back label = "+1"]
			"2" -> "1" [dir=back label = "+1"]
			"3" -> "2" [dir=back label = "+1"]

			"4" -> "3" [dir=back label = "+1"]
			"5" -> "3" [dir=back label = "+1"]

			"6" -> "4" [dir=back label = "+1"]
			"6" -> "5" [dir=back label = "+1"]

			"0" [label="Root" style=filled fillcolor=green]
		}
		 */
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(1));
			g.add(2, 3, add(1));

			g.add(3, 4, add(1));
			g.add(3, 5, add(1));
			g.add(4, 6, add(1));
			g.add(5, 6, add(1));
		});
	}

	private static int applyToState(List<TestOp> diffs) {
		TestOpState opState = new TestOpState();
		diffs.forEach(opState::apply);
		return opState.getValue();
	}
}
