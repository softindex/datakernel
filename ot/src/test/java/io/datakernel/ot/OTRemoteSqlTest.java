package io.datakernel.ot;

import ch.qos.logback.classic.Level;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.ot.OTCommit.ofCommit;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.test.TestUtils.dataSource;
import static io.datakernel.util.CollectionUtils.first;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class OTRemoteSqlTest {

	private Eventloop eventloop;
	private OTRemoteSql<TestOp> remote;
	private OTSystem<TestOp> otSystem;

	static {
		ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(Level.toLevel("TRACE"));
	}

	@Before
	public void before() throws IOException, SQLException {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		otSystem = Utils.createTestOp();
		remote = OTRemoteSql.create(eventloop, Executors.newFixedThreadPool(4), dataSource("test.properties"), otSystem, Utils.OP_ADAPTER);

		remote.truncateTables();
	}

	static <T> Set<T> set(T... values) {
		return Arrays.stream(values).collect(toSet());
	}

	private static int apply(List<TestOp> testOps) {
		TestOpState testOpState = new TestOpState();
		testOps.forEach(testOpState::apply);
		return testOpState.getValue();
	}

	@Test
	public void testJson() throws IOException {
		{
			TestAdd testAdd = new TestAdd(1);
			String json = Utils.OP_ADAPTER.toJson(testAdd);
			TestAdd testAdd2 = ((TestAdd) Utils.OP_ADAPTER.fromJson(json));
			assertEquals(testAdd.getDelta(), testAdd2.getDelta());
		}

		{
			TestSet testSet = new TestSet(0, 4);
			String json = Utils.OP_ADAPTER.toJson(testSet);
			TestSet testSet2 = ((TestSet) Utils.OP_ADAPTER.fromJson(json));
			assertEquals(testSet.getPrev(), testSet2.getPrev());
			assertEquals(testSet.getNext(), testSet2.getNext());
		}
	}

	@Test
	public void testRootHeads() throws ExecutionException, InterruptedException {
		CompletableFuture<Set<Long>> headsFuture = remote.createCommitId()
				.thenCompose(id -> remote.push(ofRoot(id)))
				.thenCompose($ -> remote.getHeads())
				.toCompletableFuture();
		eventloop.run();

		Set<Long> heads = headsFuture.get();
		assertEquals(1, heads.size());
		assertEquals(1, first(heads).intValue());
	}

	@Test
	public void testReplaceHead() throws ExecutionException, InterruptedException {
		CompletableFuture<Set<Long>> headsFuture = remote.createCommitId()
				.thenCompose(rootId -> remote.push(ofRoot(rootId))
						.thenCompose($ -> remote.createCommitId())
						.thenCompose(id -> remote.push(ofCommit(id, rootId, singletonList(new TestSet(0, 5)), id))))
				.thenCompose($ -> remote.getHeads())
				.toCompletableFuture();
		eventloop.run();

		Set<Long> heads = headsFuture.get();
		assertEquals(1, heads.size());
		assertEquals(2, first(heads).intValue());
	}

	@Test
	public void testReplaceHeadsOnMerge() throws ExecutionException, InterruptedException {
		/*
		        / firstId  \
		       /            \
		rootId -- secondId   -- mergeId(HEAD)
		       \            /
		        \ thirdId  /

		 */

		CompletableFuture<?> graphFuture = remote
				.push(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(1, 4, add(1));
				})))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		CompletableFuture<Set<Long>> headFuture = remote.getHeads().toCompletableFuture();
		eventloop.run();
		Set<Long> heads = headFuture.get();
		assertEquals(3, heads.size());
		assertEquals(set(2L, 3L, 4L), heads);

		OTAlgorithms<Long, TestOp> algorithms = new OTAlgorithms<>(eventloop, otSystem, remote);
		CompletableFuture<Long> mergeFuture = algorithms.mergeHeadsAndPush().toCompletableFuture();

		eventloop.run();
		Long mergeId = mergeFuture.get();

		CompletableFuture<Set<Long>> headAfterMergeFuture = remote.getHeads().toCompletableFuture();
		eventloop.run();
		Set<Long> headsAfterMerge = headAfterMergeFuture.get();
		assertEquals(1, headsAfterMerge.size());
		assertEquals(mergeId, first(headsAfterMerge));
	}

//	@Test
//	public void testMergeSnapshotOnMergeNodes() throws ExecutionException, InterruptedException {
//		final GraphBuilder<Long, TestOp> graphBuilder = new GraphBuilder<>(remote);
//		final CompletableFuture<Void> graphFuture = graphBuilder.buildGraph(asList(
//				edge(1, 2, add(1)),
//				edge(1, 3, add(1)),
//				edge(1, 4, add(1)),
//				edge(1, 5, add(1)),
//				edge(2, 6, add(1)),
//				edge(3, 6, add(1)),
//				edge(4, 7, add(1)),
//				edge(5, 7, add(1)),
//				edge(6, 8, add(1)),
//				edge(7, 9, add(1))))
//				.toCompletableFuture();
//
//		eventloop.run();
//		graphFuture.get();
//
//		final CompletableFuture<Long> beforeVirtualЩFutureNode4 = OTAlgorithms
//				.loadAllChanges(remote, keyComparator, otSystem, 6)
//				.thenApply(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		final CompletableFuture<Long> beforeVirtualFutureNode5 = OTAlgorithms
//				.loadAllChanges(remote, keyComparator, otSystem, 8)
//				.thenApply(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		eventloop.run();
//
//		final Long node4Before = beforeVirtualFutureNode4.get();
//		final Long node5Before = beforeVirtualFutureNode5.get();
//		assertEquals(2, node4Before.intValue());
//		assertEquals(3, node5Before.intValue());
//
//		final CompletableFuture<?> virtualFuture = OTAlgorithms
//				.saveCheckpoint(remote, keyComparator, otSystem, newHashSet(6, 7))
//				.toCompletableFuture();
//		eventloop.run();
//		virtualFuture.get();
//
//		final CompletableFuture<Long> afterVirtualFutureNode4 = OTAlgorithms
//				.loadAllChanges(remote, keyComparator, otSystem, 6)
//				.thenApply(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		final CompletableFuture<Long> afterVirtualFutureNode5 = OTAlgorithms
//				.loadAllChanges(remote, keyComparator, otSystem, 8)
//				.thenApply(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		eventloop.run();
//		final Long node4After = afterVirtualFutureNode4.get();
//		final Long node5After = afterVirtualFutureNode5.get();
//
//		assertEquals(node5Before, node5After);
//		assertEquals(node4Before, node4After);
//
//		OTAlgorithms.mergeHeadsAndPush(otSystem, remote, Long::compareTo);
//		// logs
//		eventloop.run();
//	}

	@Test
	public void testForkMerge() throws ExecutionException, InterruptedException {
		CompletableFuture<?> graphFuture = remote
				.push(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
					g.add(6, 8, add(1));
				})))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		OTAlgorithms<Long, TestOp> algorithms = new OTAlgorithms<>(eventloop, otSystem, remote);
		CompletableFuture<Long> merge = algorithms.mergeHeadsAndPush()
				.toCompletableFuture();

		eventloop.run();
		merge.get();

//		assertEquals(searchSurface, rootNodesFuture.get());
	}

	@Test
	public void testFindRootNodes() throws ExecutionException, InterruptedException {
		CompletableFuture<?> graphFuture = remote
				.push(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(2, 4, add(1));
					g.add(3, 4, add(1));
					g.add(2, 5, add(1));
					g.add(3, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
				})))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		OTAlgorithms<Long, TestOp> algorithms = new OTAlgorithms<>(eventloop, otSystem, remote);
		CompletableFuture<Set<Long>> rootNodes1Future = algorithms.findAllCommonParents(set(6L, 7L))
				.toCompletableFuture();

		CompletableFuture<Set<Long>> rootNodes2Future = algorithms.findAllCommonParents(set(6L))
				.toCompletableFuture();

		eventloop.run();

		assertEquals(set(2L, 3L), rootNodes1Future.get());
		assertEquals(set(6L), rootNodes2Future.get());
	}

	@Test
	public void testFindRootNodes2() throws ExecutionException, InterruptedException {
		CompletableFuture<?> graphFuture = remote
				.push(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(4, 6, add(1));
				})))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		OTAlgorithms<Long, TestOp> algorithms = new OTAlgorithms<>(eventloop, otSystem, remote);
		CompletableFuture<Set<Long>> rootNodesFuture = algorithms.findAllCommonParents(set(4L, 5L, 6L))
				.toCompletableFuture();

		eventloop.run();

		assertEquals(set(4L), rootNodesFuture.get());
	}

	@Test
	public void testFindParentCandidatesSurface() throws ExecutionException, InterruptedException {
		CompletableFuture<?> graphFuture = remote
				.push(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(2, 4, add(1));
					g.add(3, 4, add(1));
					g.add(2, 5, add(1));
					g.add(3, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
				})))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		Set<Long> searchSurface = set(2L, 3L);
		OTAlgorithms<Long, TestOp> algorithms = new OTAlgorithms<>(eventloop, otSystem, remote);
		CompletableFuture<Set<Long>> rootNodesFuture = algorithms.findCut(set(6L, 7L),
				commits -> searchSurface.equals(commits.stream().map(OTCommit::getId).collect(toSet())))
				.toCompletableFuture();

		eventloop.run();

		assertEquals(searchSurface, rootNodesFuture.get());
	}

	@Test
	public void testSingleCacheCheckpointNode() throws ExecutionException, InterruptedException {
		CompletableFuture<?> graphFuture = remote
				.push(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(5, 6, add(1));
					g.add(5, 7, add(1));
				})))
				.thenCompose($ -> remote.saveSnapshot(1L, emptyList()))
				.toCompletableFuture();

		eventloop.run();
		graphFuture.get();

		OTAlgorithms<Long, TestOp> algorithms = new OTAlgorithms<>(eventloop, otSystem, remote);
		CompletableFuture<?> mergeSnapshotFuture = algorithms.checkout(5L)
				.thenCompose(ds -> remote.saveSnapshot(5L, ds))
				.thenCompose($ -> remote.cleanup(5L))
				.toCompletableFuture();

		eventloop.run();
		mergeSnapshotFuture.get();

		CompletableFuture<Integer> snapshotFuture = algorithms.checkout(7L)
				.thenApply(OTRemoteSqlTest::apply)
				.toCompletableFuture();
		eventloop.run();

		assertEquals(5, snapshotFuture.get().intValue());
	}
}