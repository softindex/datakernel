package io.datakernel.ot;

import io.datakernel.ot.reducers.DiffsReducer;
import io.datakernel.ot.system.OTSystem;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.ot.utils.Utils;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static io.datakernel.common.collection.CollectionUtils.getLast;
import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.ot.OTAlgorithms.*;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTAlgorithmsTest {
	private static final Random RANDOM = new Random();
	private static final OTSystem<TestOp> TEST_OP = Utils.createTestOp();
	private static final OTRepositoryStub<Integer, TestOp> REPOSITORY = OTRepositoryStub.create();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Before
	public void reset() {
		REPOSITORY.reset();
	}

	@Test
	public void testCheckOutNoSnapshot() {
		REPOSITORY.revisionIdSupplier = () -> RANDOM.nextInt(1000) + 1;
		Integer id1 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofRoot(id1)));
		Integer id2 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofCommit(0, id2, id1, emptyList(), id1)));

		Throwable exception = awaitException(checkout(REPOSITORY, TEST_OP, id2));
		assertSame(GRAPH_EXHAUSTED, exception);
	}

	@Test
	public void testFindParentNoSnapshot() {
		REPOSITORY.revisionIdSupplier = () -> RANDOM.nextInt(1000) + 1;
		Integer id1 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofRoot(id1)));
		Integer id2 = await(REPOSITORY.createCommitId());
		await(REPOSITORY.pushAndUpdateHead(OTCommit.ofCommit(0, id2, id1, emptyList(), id1)));

		Throwable exception = awaitException(findParent(REPOSITORY, TEST_OP, singleton(id2), DiffsReducer.toVoid(),
				commit -> REPOSITORY.loadSnapshot(commit.getId())
						.map(Optional::isPresent)));
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

		await(REPOSITORY.saveSnapshot(0, asList(add(10))));

		Set<Integer> heads = await(REPOSITORY.getHeads());
		List<TestOp> changes = await(checkout(REPOSITORY, TEST_OP, getLast(heads)));
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

		Map<Integer, List<TestOp>> result = await(reduceEdges(REPOSITORY, TEST_OP, set(5, 7), 0, DiffsReducer.toList()));

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

		Map<Integer, List<TestOp>> result = await(reduceEdges(REPOSITORY, TEST_OP, set(3, 4, 5), 0, DiffsReducer.toList()));

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
		List<TestOp> diff = await(diff(REPOSITORY, TEST_OP, 5, 9));
		assertEquals(applyToState(asList(add(10))), applyToState(diff)); // -2, +4, +4, +4

		diff = await(diff(REPOSITORY, TEST_OP, 5, 0));
		assertEquals(applyToState(asList(add(-6))), applyToState(diff)); // -2, -1, -1, -1, -1

		diff = await(diff(REPOSITORY, TEST_OP, 5, 6));
		assertEquals(applyToState(asList(add(+3))), applyToState(diff)); // +3

		diff = await(diff(REPOSITORY, TEST_OP, 5, 5));
		assertEquals(emptyList(), diff); // 0

		graph2();
		diff = await(diff(REPOSITORY, TEST_OP, 6, 3));
		assertEquals(applyToState(asList(add(-2))), applyToState(diff)); // -1, -1

		diff = await(diff(REPOSITORY, TEST_OP, 0, 6));
		assertEquals(applyToState(asList(add(5))), applyToState(diff)); // +1, +1, +1, +1, +1

		diff = await(diff(REPOSITORY, TEST_OP, 4, 5));
		assertEquals(applyToState(emptyList()), applyToState(diff)); // +1, -1

		diff = await(diff(REPOSITORY, TEST_OP, 3, 2));
		assertEquals(applyToState(asList(add(-1))), applyToState(diff)); // -1

	}

	private void doTestCheckoutGraph1(int snapshotId, List<TestOp> snapshotDiffs) {
		await(REPOSITORY.saveSnapshot(snapshotId, snapshotDiffs));

		List<TestOp> diffs = await(checkout(REPOSITORY, TEST_OP, 9));
		assertEquals(16, applyToState(diffs));
	}

	private void doTestCheckoutGraph2(int snapshotId, List<TestOp> snapshotDiffs) {
		await(REPOSITORY.saveSnapshot(snapshotId, snapshotDiffs));

		List<TestOp> diffs = await(checkout(REPOSITORY, TEST_OP, 6));
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
