package io.datakernel.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.*;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.ot.OTUtils.loadAllChanges;
import static io.datakernel.ot.utils.GraphBuilder.edge;
import static io.datakernel.ot.utils.OTRemoteStub.TestSequence.of;
import static io.datakernel.ot.utils.Utils.add;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class OTAlgorithmsTest {
	private static final OTSystem<TestOp> TEST_OP = Utils.createTestOp();

	@Test
	public void testLoadAllChangesFromRootWithSnapshot() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create();
		TestOpState opState = new TestOpState();
		List<Integer> commitIds = IntStream.rangeClosed(0, 5).boxed().collect(Collectors.toList());
		OTRemote<Integer, TestOp> otRemote = OTRemoteStub.create(of(commitIds), Integer::compareTo);

		otRemote.createId().thenCompose(id -> otRemote.push(asList(OTCommit.ofRoot(id))));
		eventloop.run();
		otRemote.saveSnapshot(0, asList(add(10)));
		eventloop.run();

		commitIds.subList(0, commitIds.size() - 1).forEach(prevId -> {
			otRemote.createId().thenCompose(id -> otRemote.push(asList(OTCommit.ofCommit(id, prevId, asList(add(1))))));
			eventloop.run();
		});

		CompletableFuture<List<TestOp>> changes = otRemote.getHeads().thenCompose(heads ->
				loadAllChanges(otRemote, Integer::compareTo, TEST_OP, getLast(heads)))
				.toCompletableFuture();
		eventloop.run();
		changes.get().forEach(opState::apply);

		assertEquals(15, opState.getValue());
	}

	@Test
	public void testReduceEdges() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create();
		Comparator<Integer> comparator = Integer::compareTo;
		List<Integer> commitIds = IntStream.rangeClosed(0, 8).boxed().collect(Collectors.toList());
		OTRemote<Integer, TestOp> otRemote = OTRemoteStub.create(of(commitIds), Integer::compareTo);
		GraphBuilder<Integer, TestOp> graphBuilder = new GraphBuilder<>(otRemote);
		CompletableFuture<Map<Integer, Integer>> graphFuture = graphBuilder.buildGraph(asList(
				edge(0, 1, add(1)),
				edge(1, 2, add(1)),
				edge(2, 3, add(1)),
				edge(3, 4, add(-1)),
				edge(4, 5, add(-1)),
				edge(3, 6, add(1)),
				edge(6, 7, add(1))))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		Map<Integer, List<TestOp>> heads = Stream.of(5, 7).collect(toMap(k -> k, k -> new ArrayList<>()));
		CompletableFuture<Map<Integer, List<TestOp>>> future = OTUtils
				.reduceEdges(otRemote, comparator, heads, 0, k -> k >= 0,
						(ints, testOps) -> Stream.concat(ints.stream(), testOps.stream()).collect(toList()),
						(testOps, testOps2) -> Stream.concat(testOps.stream(), testOps2.stream()).collect(toList()))
				.toCompletableFuture();

		eventloop.run();
		Map<Integer, List<TestOp>> result = future.get();

		assertEquals(1, applyToState(result.get(5)));
		assertEquals(5, applyToState(result.get(7)));
	}

	@Test
	public void testReduceEdges2() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create();
		Comparator<Integer> comparator = Integer::compareTo;
		List<Integer> commitIds = IntStream.rangeClosed(0, 8).boxed().collect(Collectors.toList());
		OTRemote<Integer, TestOp> otRemote = OTRemoteStub.create(of(commitIds), Integer::compareTo);
		GraphBuilder<Integer, TestOp> graphBuilder = new GraphBuilder<>(otRemote);
		CompletableFuture<Map<Integer, Integer>> graphFuture = graphBuilder.buildGraph(asList(
				edge(0, 1, add(1)),
				edge(0, 2, add(-1)),
				edge(1, 3, add(1)),
				edge(1, 4, add(-1)),
				edge(2, 4, add(1)),
				edge(2, 5, add(-1))))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		Map<Integer, List<TestOp>> heads = Stream.of(3, 4, 5).collect(toMap(k -> k, k -> new ArrayList<>()));
		CompletableFuture<Map<Integer, List<TestOp>>> future = OTUtils
				.reduceEdges(otRemote, comparator, heads, 0, k -> true,
						(ints, testOps) -> Stream.concat(ints.stream(), testOps.stream()).collect(toList()),
						(testOps, testOps2) -> Stream.concat(testOps.stream(), testOps2.stream()).collect(toList()))
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