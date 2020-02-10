/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.ot;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.common.parse.ParseException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.TestAdd;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.ot.utils.TestSet;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.ot.OTAlgorithms.*;
import static io.datakernel.ot.OTCommit.ofCommit;
import static io.datakernel.ot.OTCommit.ofRoot;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.dataSource;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

@Ignore
public class OTRepositoryMySqlTest {
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private OTRepositoryMySql<TestOp> repository;
	private IdGeneratorStub idGenerator;

	static {
		Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(Level.toLevel("TRACE"));
	}

	@Before
	public void before() throws IOException, SQLException {
		idGenerator = new IdGeneratorStub();
		repository = OTRepositoryMySql.create(Eventloop.getCurrentEventloop(), Executors.newFixedThreadPool(4), dataSource("test.properties"), idGenerator,
				createTestOp(), OP_CODEC);
		repository.initialize();
		repository.truncateTables();
	}

	@SafeVarargs
	private static <T> Set<T> set(T... values) {
		return Arrays.stream(values).collect(toSet());
	}

	private static int apply(List<TestOp> testOps) {
		TestOpState testOpState = new TestOpState();
		testOps.forEach(testOpState::apply);
		return testOpState.getValue();
	}

	@Test
	public void testJson() throws ParseException {
		{
			TestAdd testAdd = new TestAdd(1);
			String json = toJson(OP_CODEC, testAdd);
			TestAdd testAdd2 = (TestAdd) fromJson(OP_CODEC, json);
			assertEquals(testAdd.getDelta(), testAdd2.getDelta());
		}

		{
			TestSet testSet = new TestSet(0, 4);
			String json = toJson(OP_CODEC, testSet);
			TestSet testSet2 = (TestSet) fromJson(OP_CODEC, json);
			assertEquals(testSet.getPrev(), testSet2.getPrev());
			assertEquals(testSet.getNext(), testSet2.getNext());
		}
	}

	@Test
	public void testRootHeads() {
		Long id = await(repository.createCommitId());
		await(repository.pushAndUpdateHead(ofRoot(id)));

		Set<Long> heads = await(repository.getHeads());
		assertEquals(1, heads.size());
		assertEquals(1, first(heads).intValue());
	}

	@Test
	public void testReplaceHead() {
		Long rootId = await(repository.createCommitId());
		await(repository.pushAndUpdateHead(ofRoot(rootId)));

		Long id = await(repository.createCommitId());

		await(repository.pushAndUpdateHead(ofCommit(0, id, rootId, singletonList(new TestSet(0, 5)), id)));

		Set<Long> heads = await(repository.getHeads());
		assertEquals(1, heads.size());
		assertEquals(2, first(heads).intValue());
	}

	@Test
	public void testReplaceHeadsOnMerge() {
		/*
		        / firstId  \
		       /            \
		rootId -- secondId   -- mergeId(HEAD)
		       \            /
		        \ thirdId  /

		 */

		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(1, 4, add(1));
				}))));
		idGenerator.set(4);

		Set<Long> heads = await(repository.getHeads());
		assertEquals(3, heads.size());
		assertEquals(set(2L, 3L, 4L), heads);

		Long mergeId = await(mergeAndUpdateHeads(repository, SYSTEM));

		Set<Long> headsAfterMerge = await(repository.getHeads());
		assertEquals(1, headsAfterMerge.size());
		assertEquals(mergeId, first(headsAfterMerge));
	}

	//	@Test
//	public void testMergeSnapshotOnMergeNodes() throws ExecutionException, InterruptedException {
//		final GraphBuilder<Long, TestOp> graphBuilder = new GraphBuilder<>(repository);
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
//				.loadAllChanges(repository, keyComparator, otSystem, 6)
//				.map(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		final CompletableFuture<Long> beforeVirtualFutureNode5 = OTAlgorithms
//				.loadAllChanges(repository, keyComparator, otSystem, 8)
//				.map(OTRemoteSqlTest::apply)
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
//				.saveCheckpoint(repository, keyComparator, otSystem, newHashSet(6, 7))
//				.toCompletableFuture();
//		eventloop.run();
//		virtualFuture.get();
//
//		final CompletableFuture<Long> afterVirtualFutureNode4 = OTAlgorithms
//				.loadAllChanges(repository, keyComparator, otSystem, 6)
//				.map(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		final CompletableFuture<Long> afterVirtualFutureNode5 = OTAlgorithms
//				.loadAllChanges(repository, keyComparator, otSystem, 8)
//				.map(OTRemoteSqlTest::apply)
//				.toCompletableFuture();
//
//		eventloop.run();
//		final Long node4After = afterVirtualFutureNode4.get();
//		final Long node5After = afterVirtualFutureNode5.get();
//
//		assertEquals(node5Before, node5After);
//		assertEquals(node4Before, node4After);
//
//		OTAlgorithms.mergeHeadsAndPush(otSystem, repository, Long::compareTo);
//		// logs
//		eventloop.run();
//	}

	@Test
	public void testForkMerge() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
					g.add(6, 8, add(1));
				}))));
		idGenerator.set(8);

		await(mergeAndUpdateHeads(repository, SYSTEM));
		//		assertEquals(searchSurface, rootNodesFuture.get());
	}

	@Test
	public void testFindRootNodes() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(2, 4, add(1));
					g.add(3, 4, add(1));
					g.add(2, 5, add(1));
					g.add(3, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
				}))));

		assertEquals(set(2L, 3L), await(findAllCommonParents(repository, SYSTEM, set(6L, 7L))));
		assertEquals(set(6L), await(findAllCommonParents(repository, SYSTEM, set(6L))));
	}

	@Test
	public void testFindRootNodes2() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(4, 6, add(1));
				}))));

		assertEquals(set(4L), await(findAllCommonParents(repository, SYSTEM, set(4L, 5L, 6L))));
	}

	@Test
	public void testFindParentCandidatesSurface() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(1, 3, add(1));
					g.add(2, 4, add(1));
					g.add(3, 4, add(1));
					g.add(2, 5, add(1));
					g.add(3, 5, add(1));
					g.add(4, 6, add(1));
					g.add(5, 7, add(1));
				}))));

		Set<Long> searchSurface = set(2L, 3L);

		Set<Long> rootNodes = await(findCut(repository, SYSTEM, set(6L, 7L),
				commits -> searchSurface.equals(commits.stream().map(OTCommit::getId).collect(toSet()))));

		assertEquals(searchSurface, rootNodes);
	}

	@Test
	public void testSingleCacheCheckpointNode() {
		await(repository
				.pushAndUpdateHeads(commits(asLong(g -> {
					g.add(1, 2, add(1));
					g.add(2, 3, add(1));
					g.add(3, 4, add(1));
					g.add(4, 5, add(1));
					g.add(5, 6, add(1));
					g.add(5, 7, add(1));
				}))));
		await(repository.saveSnapshot(1L, emptyList()));

		List<TestOp> diffs = await(checkout(repository, SYSTEM, 5L));

		await(repository.saveSnapshot(5L, diffs));
		await(repository.cleanup(5L));

		int result = apply(await(checkout(repository, SYSTEM, 7L)));
		assertEquals(5, result);
	}
}
