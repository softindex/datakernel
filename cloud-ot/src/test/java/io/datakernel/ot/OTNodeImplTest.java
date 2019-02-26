package io.datakernel.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.ot.OTNode.FetchData;
import io.datakernel.ot.utils.OTGraphBuilder;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.function.Consumer;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.ot.utils.Utils.createTestOp;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(DatakernelRunner.class)
public class OTNodeImplTest {
	private static final TestOpState state = new TestOpState();
	private final OTRepositoryStub<Integer, TestOp> REPOSITORY = OTRepositoryStub.create();

	private OTNode<Integer, TestOp, OTCommit<Integer, TestOp>> node;

	@Before
	public void setUp() {
		REPOSITORY.reset();
		OTAlgorithms<Integer, TestOp> algorithms = OTAlgorithms.create(Eventloop.getCurrentEventloop(), createTestOp(), REPOSITORY);
		node = algorithms.getOtNode();
		resetRepo(null);

	}

	@Test
	public void testFetchLinearGraph() {
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));
			g.add(3, 4, add(4));
			g.add(4, 5, add(5));
			g.add(5, 6, add(6));
		});

		FetchData<Integer, TestOp> fetchData1 = await(node.fetch(0));
		assertFetchData(6, 7, 21, fetchData1);

		FetchData<Integer, TestOp> fetchData2 = await(node.fetch(3));
		assertFetchData(6, 7, 15, fetchData2);
	}

	// FIXME
	@Ignore
	@Test
	public void testFetch2BranchesGraph() {
		REPOSITORY.revisionIdSupplier = () -> 6; // id of merge commit

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData1 = await(node.fetch(0));
		assertFetchData(6, 5, 15, fetchData1);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData2 = await(node.fetch(1));
		assertFetchData(6, 5, 14, fetchData2);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData3 = await(node.fetch(4));
		assertFetchData(6, 5, 11, fetchData3);
	}

	@Test
	public void testFetchSplittingGraph() {
		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));

			g.add(3, 6, add(9));
			g.add(5, 6, add(6));

			g.add(6, 7, add(7));
		});

		FetchData<Integer, TestOp> fetchData1 = await(node.fetch(0));
		assertFetchData(7, 6, 22, fetchData1);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));

			g.add(3, 6, add(9));
			g.add(5, 6, add(6));

			g.add(6, 7, add(7));
		});

		FetchData<Integer, TestOp> fetchData2 = await(node.fetch(1));
		assertFetchData(7, 6, 21, fetchData2);

		resetRepo(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));

			g.add(3, 6, add(9));
			g.add(5, 6, add(6));

			g.add(6, 7, add(7));
		});

		FetchData<Integer, TestOp> fetchData3 = await(node.fetch(4));
		assertFetchData(7, 6, 18, fetchData3);
	}

	@Test
	public void testFetchInvalidRevision() {
		Throwable exception = awaitException(node.fetch(100));
		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString("Graph exhausted"));
	}

	@Test
	public void testCheckoutEmptyGraph() {
		FetchData<Integer, TestOp> fetchData = await(node.checkout());
		assertFetchData(0, 1, 0, fetchData);
	}

	@Test
	public void testCheckoutLinearGraph() {
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));
			g.add(3, 4, add(4));
			g.add(4, 5, add(5));
			g.add(5, 6, add(6));
		});

		FetchData<Integer, TestOp> fetchData = await(node.checkout());
		assertFetchData(6, 7, 21, fetchData);
	}

	@Test
	public void testCheckout2BranchesGraph() {
		REPOSITORY.revisionIdSupplier = () -> 6; // id of merge commit
		REPOSITORY.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));

			g.add(0, 4, add(4));
			g.add(4, 5, add(5));
		});

		FetchData<Integer, TestOp> fetchData = await(node.checkout());
		assertFetchData(6, 5, 15, fetchData);

		// Additional snapshot in branch1
		REPOSITORY.saveSnapshot(4, singletonList(add(4)));

		FetchData<Integer, TestOp> fetchData2 = await(node.checkout());
		assertFetchData(6, 5, 15, fetchData2);

		// Additional snapshot in branch2
		REPOSITORY.saveSnapshot(1, singletonList(add(1)));

		FetchData<Integer, TestOp> fetchData3 = await(node.checkout());
		assertFetchData(6, 5, 15, fetchData3);

	}

	private static void assertFetchData(Integer expectedId, long expectedLevel, Integer expectedState, FetchData<Integer, TestOp> fetchData) {
		assertEquals(expectedId, fetchData.getCommitId());
		assertEquals(expectedLevel, fetchData.getLevel());
		state.init();
		fetchData.getDiffs().forEach(state::apply);
		assertEquals(expectedState, (Integer) state.getValue());
	}

	private void resetRepo(Consumer<OTGraphBuilder<Integer, TestOp>> builder) {
		// Initializing repo
		await(REPOSITORY.push(ofRoot(0)), ((OTRepository<Integer, TestOp>) REPOSITORY).saveSnapshot(0, emptyList()));

		if (builder != null) {
			REPOSITORY.setGraph(builder);
		}
	}

}
