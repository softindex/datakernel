package io.datakernel.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.OTRemoteStub;
import io.datakernel.ot.utils.TestOp;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.ot.utils.Utils.add;
import static io.datakernel.ot.utils.Utils.createTestOp;
import static io.datakernel.util.CollectionUtils.set;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTMergeAlgorithmTest {

	public interface OTGraphAdapter<K, D> {
		void add(K parent, K child, List<D> diffs);

		default void add(K parent, K child, D diff) {
			add(parent, child, asList(diff));
		}
	}

	public interface OTGraphBuilder<K, D> {
		void build(OTGraphAdapter<K, D> graph);
	}

	static <K, D> OTLoadedGraph<K, D> buildGraph(OTGraphBuilder<K, D> builder) {
		OTLoadedGraph<K, D> graph = new OTLoadedGraph<>();
		builder.build(new OTGraphAdapter<K, D>() {
			@Override
			public void add(K parent, K child, List<D> diffs) {
				checkArgument(graph.getParents(child) == null || graph.getParents(child).get(parent) == null);
				graph.add(parent, child, diffs);
			}
		});
		return graph;
	}

	static <K, D> OTRemote<K, D> buildRemote(OTGraphBuilder<K, D> builder, Comparator<K> comparator) {
		OTRemoteStub<K, D> remote = OTRemoteStub.create(comparator);
		builder.build(new OTGraphAdapter<K, D>() {
			@Override
			public void add(K parent, K child, List<D> diffs) {
				remote.add(parent, child, diffs);
			}
		});
		return remote;
	}

	private Map<String, List<TestOp>> doTest(Set<String> heads, OTGraphBuilder<String, TestOp> graphBuilder) throws Exception {
		OTSystem<TestOp> system = createTestOp();
		Comparator<String> comparator = (o1, o2) -> o1.compareTo(o2);
		OTRemote<String, TestOp> otRemote = buildRemote(graphBuilder, comparator);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		CompletableFuture<OTLoadedGraph<String, TestOp>> future =
				OTMergeAlgorithm.loadGraph(otRemote, comparator, heads).toCompletableFuture();
		eventloop.run();
		OTLoadedGraph<String, TestOp> graph = future.get();
		System.out.println(graph.toGraphViz());

		Map<String, List<TestOp>> merge = OTMergeAlgorithm.merge(system, comparator, graph, heads);
		System.out.println(graph.toGraphViz());
		return merge;
	}

	@Test
	// Merge one node should return empty merge
	public void test1() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("A", "B"), g -> {
			g.add("A", "B", add(1));
		});
		assertEquals(list(add(1)), merge.get("A"));
		assertEquals(list(), merge.get("B"));
	}

	@Test
	// Merge already merged line
	public void test2() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("A", "B"), g -> {
			g.add("A", "T", add(10));
			g.add("T", "B", add(1));
		});
		assertEquals(list(add(11)), merge.get("A"));
		assertEquals(list(), merge.get("B"));
	}

	@Test
	// Merge V form tree
	public void test3() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("D", "E"), g -> {
			g.add("A", "B", add(1));
			g.add("A", "C", add(100));
			g.add("B", "D", add(10));
			g.add("C", "E", add(1000));
		});
		assertEquals(list(add(1100)), merge.get("D"));
		assertEquals(list(add(11)), merge.get("E"));
	}

	@Test
	// Merge A, B nodes and D, E subnodes
	public void test4() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("A", "B", "D", "E"), g -> {
			g.add("A", "C", add(1));
			g.add("B", "C", add(3));
			g.add("B", "D", add(-5));
			g.add("C", "E", add(10));
		});
		assertEquals(list(add(-5)), merge.get("E"));
		assertEquals(list(add(13)), merge.get("D"));
		assertEquals(list(add(6)), merge.get("A"));
		assertEquals(list(add(8)), merge.get("B"));
	}

	@Test
	// Merge triple form tree
	public void test5() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("A", "B", "C"), g -> {
			g.add("*", "A", add(1));
			g.add("*", "B", add(10));
			g.add("*", "C", add(100));
		});
		assertEquals(list(add(110)), merge.get("A"));
		assertEquals(list(add(101)), merge.get("B"));
		assertEquals(list(add(11)), merge.get("C"));
	}

	@Test
	// Merge W form graph
	public void test6() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("C", "D", "E"), g -> {
			g.add("A", "C", add(3));
			g.add("A", "D", add(10));
			g.add("B", "D", add(1));
			g.add("B", "E", add(30));
		});
		assertEquals(list(add(40)), merge.get("C"));
		assertEquals(list(add(33)), merge.get("D"));
		assertEquals(list(add(4)), merge.get("E"));
	}

	@Test
	// Merge equal merges of two nodes
	public void test7() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("C", "D"), g -> {
			g.add("A", "C", add(2));
			g.add("A", "D", add(2));
			g.add("B", "C", add(1));
			g.add("B", "D", add(1));
		});
		assertEquals(list(), merge.get("C"));
		assertEquals(list(), merge.get("D"));
	}

	@Test
	// Merge three equal merges on three nodes
	public void test7a() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("D", "E", "F"), g -> {
			g.add("A", "D", add(5));
			g.add("A", "E", add(5));
			g.add("A", "F", add(5));
			g.add("B", "D", add(4));
			g.add("B", "E", add(4));
			g.add("B", "F", add(4));
			g.add("C", "D", add(3));
			g.add("C", "E", add(3));
			g.add("C", "F", add(3));
		});
		assertEquals(list(), merge.get("D"));
		assertEquals(list(), merge.get("E"));
		assertEquals(list(), merge.get("F"));
	}

	@Test
	// Merge full merge and submerge
	public void test8() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("E", "F"), g -> {
			g.add("A", "C", add(10));
			g.add("A", "D", add(100));
			g.add("B", "E", add(10));
			g.add("B", "F", add(110));
			g.add("C", "E", add(1));
			g.add("C", "F", add(101));
			g.add("D", "F", add(11));
		});
		assertEquals(list(add(100)), merge.get("E"));
		assertEquals(list(), merge.get("F"));
	}

	@Test
	// Merge two submerges
	public void test9() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("G", "J"), g -> {
			g.add("A", "C", add(1));
			g.add("A", "D", add(10));
			g.add("B", "E", add(100));
			g.add("B", "F", add(1000));
			g.add("C", "G", add(112));
			g.add("D", "G", add(103));
			g.add("D", "J", add(1102));
			g.add("E", "G", add(14));
			g.add("E", "J", add(1013));
			g.add("F", "J", add(113));
		});
		assertEquals(list(add(1000)), merge.get("G"));
		assertEquals(list(add(1)), merge.get("J"));
	}

	@Test
	// Merge having equal merges parents
	public void test10() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("E", "F", "G"), g -> {
			g.add("A", "C", add(3));
			g.add("A", "D", add(3));
			g.add("B", "C", add(2));
			g.add("B", "D", add(2));
			g.add("C", "E", add(1));
			g.add("C", "F", add(10));
			g.add("D", "G", add(100));
		});
		assertEquals(list(add(110)), merge.get("E"));
		assertEquals(list(add(101)), merge.get("F"));
		assertEquals(list(add(11)), merge.get("G"));
	}

	@Test
	// Merge having equal merges parents
	public void test11() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("I", "J"), g -> {
			g.add("A", "C", add(1));
			g.add("A", "D", add(10));
			g.add("B", "E", add(100));
			g.add("B", "F", add(1000));
			g.add("C", "G", add(112));
			g.add("D", "G", add(103));
			g.add("D", "H", add(1102));
			g.add("E", "G", add(14));
			g.add("E", "H", add(1013));
			g.add("F", "H", add(113));
			g.add("G", "I", add(-10));
			g.add("H", "J", add(-100));
		});
		assertEquals(list(add(900)), merge.get("I"));
		assertEquals(list(add(-9)), merge.get("J"));
	}

	@Test
	// Merge of merges should check operations
	public void test12() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("F", "G"), g -> {
			g.add("A", "D", add(1));
			g.add("A", "B", add(100));
			g.add("B", "C", add(2));
			g.add("B", "E", add(3));
			g.add("C", "F", add(1));
			g.add("D", "F", add(102));
			g.add("D", "G", add(103));
			g.add("E", "G", add(1));
		});
		assertEquals(list(add(3)), merge.get("F"));
		assertEquals(list(add(2)), merge.get("G"));
	}

	@Test
	// Should merge in different order
	public void test13() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("F", "C", "E"), g -> {
			g.add("A", "C", add(3));
			g.add("A", "D", add(10));
			g.add("B", "D", add(1));
			g.add("B", "E", add(30));
			g.add("D", "F", add(5));
		});
		assertEquals(list(add(45)), merge.get("C"));
		assertEquals(list(add(33)), merge.get("F"));
		assertEquals(list(add(9)), merge.get("E"));
	}

	@Test
	public void test14() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("X", "Y", "Z"), g -> {
			g.add("A", "X", add(1));
			g.add("A", "Z", add(2));
			g.add("B", "X", add(1));
			g.add("B", "Y", add(1));
			g.add("C", "Y", add(1));
			g.add("C", "Z", add(2));
			g.add("B", "Z", add(2));
		});
		System.out.println(merge);
	}

	@Test
	public void test15() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("X", "Z"), g -> {
			g.add("A", "U", add(10));
			g.add("A", "Z", add(2));
			g.add("B", "X", add(1));
			g.add("B", "Z", add(2));
			g.add("C", "V", add(100));
			g.add("U", "X", add(-9));
			g.add("V", "Z", add(-98));
		});
		System.out.println(merge);
	}

	@Test
	public void test16() throws Exception {
		Map<String, List<TestOp>> merge = doTest(set("X", "Y"), g -> {
			g.add("A", "U", add(10));
			g.add("A", "Y", add(2));
			g.add("B", "X", add(1));
			g.add("B", "V", add(100));
			g.add("U", "X", add(-9));
			g.add("V", "Y", add(-98));
		});
		System.out.println(merge);
	}

	private static <T> List<T> list() {
		return emptyList();
	}

	@SuppressWarnings("unchecked")
	private static <T> List<T> list(T... items) {
		return asList(items);
	}

}