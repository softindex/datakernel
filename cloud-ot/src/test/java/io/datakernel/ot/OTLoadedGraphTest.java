package io.datakernel.ot;

import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.ot.OTAlgorithms.loadGraph;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.ot.utils.Utils.createTestOp;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class OTLoadedGraphTest {
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private OTRepositoryStub<Integer, TestOp> repository;

	@Before
	public void setUp() {
		repository = OTRepositoryStub.create();
		await(repository.pushAndUpdateHead(ofRoot(0)), repository.saveSnapshot(0, emptyList()));
	}

	@Test
	public void testCleanUpLinearGraph() {
		repository.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));
			g.add(3, 4, add(4));
			g.add(4, 5, add(5));
			g.add(5, 6, add(6));
			g.add(6, 7, add(7));
		});

		Set<Integer> heads = await(repository.getHeads());
		OTLoadedGraph<Integer, TestOp> graph = await(loadGraph(repository, SYSTEM, heads));

		assertEquals(set(7), graph.getTips());
		assertEquals(set(0), graph.getRoots());
	}

	@Test
	public void testCleanUpSplittingGraph() {
		repository.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));
			g.add(3, 4, add(4));

			g.add(0, 5, add(5));
			g.add(5, 6, add(6));
			g.add(6, 7, add(7));
		});

		Set<Integer> heads = await(repository.getHeads());
		OTLoadedGraph<Integer, TestOp> graph = await(loadGraph(repository, SYSTEM, heads));

		assertEquals(set(4, 7), graph.getTips());
		assertEquals(set(0), graph.getRoots());
	}

	@Test
	public void testIncrementalLoading() {
		repository.setGraph(g -> {
			g.add(0, 1, add(1));
			g.add(1, 2, add(2));
			g.add(2, 3, add(3));
			g.add(3, 4, add(4));
			g.add(4, 5, add(5));
			g.add(5, 6, add(6));
			g.add(6, 7, add(7));
		});

		Set<Integer> heads = await(repository.getHeads());
		OTLoadedGraph<Integer, TestOp> graph = await(loadGraph(repository, SYSTEM, heads));

		assertEquals(set(7), graph.getTips());

		repository.addGraph(g -> g.add(7, 8, add(8)));

		heads = await(repository.getHeads());
		await(loadGraph(repository, SYSTEM, heads, graph));

		assertEquals(set(8), graph.getTips());
	}
}
