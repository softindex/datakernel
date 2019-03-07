package io.datakernel.ot;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.ot.utils.Utils;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.ot.OTAlgorithms.GRAPH_EXHAUSTED;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.util.CollectionUtils.getLast;
import static io.datakernel.util.CollectionUtils.set;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(DatakernelRunner.class)
@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTAlgorithmsTest {
	private static final Random RANDOM = new Random();
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
	public void testCheckOutNoSnapshot() {
		REPOSITORY.revisionIdSupplier = () -> RANDOM.nextInt(1000) + 1;
		Integer id1 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofRoot(id1)));
		Integer id2 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofCommit(id2, id1, emptyList(), id1)));

		Throwable exception = awaitException(algorithms.checkout(id2));
		assertSame(GRAPH_EXHAUSTED, exception);
	}

	@Test
	public void testFindParentNoSnapshot() {
		REPOSITORY.revisionIdSupplier = () -> RANDOM.nextInt(1000) + 1;
		Integer id1 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofRoot(id1)));
		Integer id2 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofCommit(id2, id1, emptyList(), id1)));

		Throwable exception = awaitException(algorithms.findParent(singleton(id2), DiffsReducer.toVoid(),
				commit -> commit.getSnapshotHint() != null ?
						Promise.of(commit.getSnapshotHint()) :
						REPOSITORY.loadSnapshot(commit.getId())
								.thenApply(Optional::isPresent)));
		assertSame(GRAPH_EXHAUSTED, exception);
	}

	@Test
	public void testLoadAllChangesFromRootWithSnapshot() {
		TestOpState opState = new TestOpState();
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(1));
			g.add(2, 3, add(1));
			g.add(3, 4, add(1));
			g.add(4, 5, add(1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, REPOSITORY);

		await(REPOSITORY.saveSnapshot(0, asList(add(10))));

		Set<Integer> heads = await(REPOSITORY.getHeads());
		List<TestOp> changes = await(algorithms.checkout(getLast(heads)));
		changes.forEach(opState::apply);

		assertEquals(15, opState.getValue());
	}

	@Test
	public void testReduceEdges() {
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

		Map<Integer, List<TestOp>> result = await(algorithms.reduceEdges(set(5, 7), 0, DiffsReducer.toList()));

		assertEquals(1, applyToState(result.get(5)));
		assertEquals(5, applyToState(result.get(7)));
	}

	@Test
	public void testReduceEdges2() {
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(0, 2, add(-1));
			g.add(1, 3, add(1));
			g.add(1, 4, add(-1));
			g.add(2, 4, add(1));
			g.add(2, 5, add(-1));
		});
		OTAlgorithms<Integer, TestOp> algorithms = new OTAlgorithms<>(eventloop, TEST_OP, REPOSITORY);

		Map<Integer, List<TestOp>> result = await(algorithms.reduceEdges(set(3, 4, 5), 0, DiffsReducer.toList()));

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
		List<TestOp> diff = await(algorithms.diff(5, 9));
		assertEquals(applyToState(asList(add(10))), applyToState(diff)); // -2, +4, +4, +4

		diff = await(algorithms.diff(5, 0));
		assertEquals(applyToState(asList(add(-6))), applyToState(diff)); // -2, -1, -1, -1, -1

		diff = await(algorithms.diff(5, 6));
		assertEquals(applyToState(asList(add(+3))), applyToState(diff)); // +3

		diff = await(algorithms.diff(5, 5));
		assertEquals(emptyList(), diff); // 0

		graph2();
		diff = await(algorithms.diff(6, 3));
		assertEquals(applyToState(asList(add(-2))), applyToState(diff)); // -1, -1

		diff = await(algorithms.diff(0, 6));
		assertEquals(applyToState(asList(add(5))), applyToState(diff)); // +1, +1, +1, +1, +1

		diff = await(algorithms.diff(4, 5));
		assertEquals(applyToState(emptyList()), applyToState(diff)); // +1, -1

		diff = await(algorithms.diff(3, 2));
		assertEquals(applyToState(asList(add(-1))), applyToState(diff)); // -1

	}

	private void doTestCheckoutGraph1(int snaphotId, List<TestOp> snapshotDiffs) {
		await(REPOSITORY.saveSnapshot(snaphotId, snapshotDiffs));

		List<TestOp> diffs = await(algorithms.checkout(9));
		assertEquals(16, applyToState(diffs));
	}

	private void doTestCheckoutGraph2(int snaphotId, List<TestOp> snapshotDiffs) {
		await(REPOSITORY.saveSnapshot(snaphotId, snapshotDiffs));

		List<TestOp> diffs = await(algorithms.checkout(6));
		assertEquals(5, applyToState(diffs));
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
