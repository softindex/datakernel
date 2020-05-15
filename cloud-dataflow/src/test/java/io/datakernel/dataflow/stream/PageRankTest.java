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

package io.datakernel.dataflow.stream;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.dataset.SortedDataset;
import io.datakernel.dataflow.dataset.impl.DatasetListConsumer;
import io.datakernel.dataflow.dsl.AST;
import io.datakernel.dataflow.dsl.EvaluationContext;
import io.datakernel.dataflow.dsl.DslParser;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.datakernel.dataflow.server.DataflowServer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.processor.StreamJoin.InnerJoiner;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult;
import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.dataflow.di.EnvironmentModule.slot;
import static io.datakernel.dataflow.dsl.DslParser.defaultExpressions;
import static io.datakernel.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.datakernel.dataflow.stream.DataflowTest.createCommon;
import static io.datakernel.dataflow.stream.DataflowTest.getFreeListenAddress;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class PageRankTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor;

	@Before
	public void setUp() {
		executor = Executors.newSingleThreadExecutor();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	public static class Page {
		@Serialize(order = 0)
		public final long pageId;
		@Serialize(order = 1)
		public final long[] links;

		public Page(@Deserialize("pageId") long pageId, @Deserialize("links") long[] links) {
			this.pageId = pageId;
			this.links = links;
		}

		public void disperse(Rank rank, StreamDataAcceptor<Rank> cb) {
			for (long link : links) {
				Rank newRank = new Rank(link, rank.value / links.length);
				cb.accept(newRank);
			}
		}

		@Override
		public String toString() {
			return "Page{pageId=" + pageId + ", links=" + Arrays.toString(links) + '}';
		}
	}

	public static class PageKeyFunction implements Function<Page, Long> {
		@Override
		public Long apply(Page page) {
			return page.pageId;
		}
	}

	public static class Rank {
		@Serialize(order = 0)
		public final long pageId;
		@Serialize(order = 1)
		public final double value;

		public Rank(@Deserialize("pageId") long pageId, @Deserialize("value") double value) {
			this.pageId = pageId;
			this.value = value;
		}

		@Override
		public String toString() {
			return "Rank{pageId=" + pageId + ", value=" + value + '}';
		}

		@SuppressWarnings({"SimplifiableIfStatement", "EqualsWhichDoesntCheckParameterClass"})
		@Override
		public boolean equals(Object o) {
			Rank rank = (Rank) o;
			if (pageId != rank.pageId) return false;
			return Math.abs(rank.value - value) < 0.001;
		}
	}

	public static class RankKeyFunction implements Function<Rank, Long> {
		@Override
		public Long apply(Rank rank) {
			return rank.pageId;
		}
	}

	public static class RankAccumulator {
		@Serialize(order = 0)
		public long pageId;
		@Serialize(order = 1)
		public double accumulatedRank;

		@SuppressWarnings("unused")
		public RankAccumulator() {
		}

		public RankAccumulator(long pageId) {
			this.pageId = pageId;
		}

		@Override
		public String toString() {
			return "RankAccumulator{pageId=" + pageId + ", accumulatedRank=" + accumulatedRank + '}';
		}
	}

	public static class RankAccumulatorKeyFunction implements Function<RankAccumulator, Long> {
		@Override
		public Long apply(RankAccumulator rankAccumulator) {
			return rankAccumulator.pageId;
		}
	}

	private static class RankAccumulatorReducer extends ReducerToResult<Long, Rank, Rank, RankAccumulator> {
		@Override
		public RankAccumulator createAccumulator(Long pageId) {
			return new RankAccumulator(pageId);
		}

		@Override
		public RankAccumulator accumulate(RankAccumulator accumulator, Rank value) {
			accumulator.accumulatedRank += value.value;
			return accumulator;
		}

		@Override
		public RankAccumulator combine(RankAccumulator accumulator, RankAccumulator anotherAccumulator) {
			accumulator.accumulatedRank += anotherAccumulator.accumulatedRank;
			return accumulator;
		}

		@Override
		public Rank produceResult(RankAccumulator accumulator) {
			return new Rank(accumulator.pageId, accumulator.accumulatedRank);
		}
	}

	public static class LongComparator implements Comparator<Long> {
		@Override
		public int compare(Long l1, Long l2) {
			return l1.compareTo(l2);
		}
	}

	public static class PageToRankFunction implements Function<Page, Rank> {
		@Override
		public Rank apply(Page page) {
			return new Rank(page.pageId, 1.0);
		}
	}

	public static class PageRankJoiner extends InnerJoiner<Long, Page, Rank, Rank> {
		@Override
		public void onInnerJoin(Long key, Page page, Rank rank, StreamDataAcceptor<Rank> output) {
			page.disperse(rank, output);
		}
	}

	private static SortedDataset<Long, Rank> pageRankIteration(SortedDataset<Long, Page> pages, SortedDataset<Long, Rank> ranks) {
		Dataset<Rank> updates = join(pages, ranks, new PageRankJoiner(), Rank.class, new RankKeyFunction());

		Dataset<Rank> newRanks = sort_Reduce_Repartition_Reduce(updates, new RankAccumulatorReducer(),
				Long.class, new RankKeyFunction(), new LongComparator(),
				RankAccumulator.class, new RankAccumulatorKeyFunction(),
				Rank.class);

		return castToSorted(newRanks, Long.class, new RankKeyFunction(), new LongComparator());
	}

	private static SortedDataset<Long, Rank> pageRank(SortedDataset<Long, Page> pages) {
		SortedDataset<Long, Rank> ranks = castToSorted(map(pages, new PageToRankFunction(), Rank.class),
				Long.class, new RankKeyFunction(), new LongComparator());

		for (int i = 0; i < 10; i++) {
			ranks = pageRankIteration(pages, ranks);
		}

		return ranks;
	}

	@Test
	public void test() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.bind(new Key<StructuredCodec<PageKeyFunction>>() {}).toInstance(object(PageKeyFunction::new))
				.bind(new Key<StructuredCodec<RankKeyFunction>>() {}).toInstance(object(RankKeyFunction::new))
				.bind(new Key<StructuredCodec<RankAccumulatorKeyFunction>>() {}).toInstance(object(RankAccumulatorKeyFunction::new))
				.bind(new Key<StructuredCodec<LongComparator>>() {}).toInstance(object(LongComparator::new))
				.bind(new Key<StructuredCodec<PageToRankFunction>>() {}).toInstance(object(PageToRankFunction::new))
				.bind(new Key<StructuredCodec<RankAccumulatorReducer>>() {}).toInstance(object(RankAccumulatorReducer::new))
				.bind(new Key<StructuredCodec<PageRankJoiner>>() {}).toInstance(object(PageRankJoiner::new))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<Rank> result1 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(asList(
						new Page(1, new long[]{1, 2, 3}),
						new Page(3, new long[]{1})))
				.bind(slot("result")).toInstance(result1)

				.build();

		StreamConsumerToList<Rank> result2 = StreamConsumerToList.create();
		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(singletonList(
						new Page(2, new long[]{1})))
				.bind(slot("result")).toInstance(result2)

				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		SortedDataset<Long, Page> pages = repartition_Sort(sortedDatasetOfList("items",
				Page.class, Long.class, new PageKeyFunction(), new LongComparator()));

		SortedDataset<Long, Rank> pageRanks = pageRank(pages);

		DatasetListConsumer<?> consumerNode = listConsumer(pageRanks, "result");

		consumerNode.compileInto(graph);

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		List<Rank> result = new ArrayList<>();
		result.addAll(result1.getList());
		result.addAll(result2.getList());
		result.sort(Comparator.comparingLong(rank -> rank.pageId));

		assertEquals(asList(new Rank(1, 1.7861), new Rank(2, 0.6069), new Rank(3, 0.6069)), result);
	}

	@Test
	public void testQL() throws Exception {
		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.bind(new Key<StructuredCodec<PageKeyFunction>>() {}).toInstance(object(PageKeyFunction::new))
				.bind(new Key<StructuredCodec<RankKeyFunction>>() {}).toInstance(object(RankKeyFunction::new))
				.bind(new Key<StructuredCodec<RankAccumulatorKeyFunction>>() {}).toInstance(object(RankAccumulatorKeyFunction::new))
				.bind(new Key<StructuredCodec<LongComparator>>() {}).toInstance(object(LongComparator::new))
				.bind(new Key<StructuredCodec<PageToRankFunction>>() {}).toInstance(object(PageToRankFunction::new))
				.bind(new Key<StructuredCodec<RankAccumulatorReducer>>() {}).toInstance(object(RankAccumulatorReducer::new))
				.bind(new Key<StructuredCodec<PageRankJoiner>>() {}).toInstance(object(PageRankJoiner::new))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		StreamConsumerToList<Rank> result1 = StreamConsumerToList.create();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(asList(
						new Page(1, new long[]{1, 2, 3}),
						new Page(3, new long[]{1})))
				.bind(slot("result")).toInstance(result1)

				.build();

		StreamConsumerToList<Rank> result2 = StreamConsumerToList.create();
		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(singletonList(
						new Page(2, new long[]{1})))
				.bind(slot("result")).toInstance(result2)

				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		AST.Query query = DslParser.create(defaultExpressions()).parse(
				"USE \"io.datakernel.dataflow.stream.PageRankTest$\"\n" +
						"\n" +
						"pages = DATASET \"items\" TYPE \"Page\"\n" +
						"pages = CAST SORT pages BY \"PageKeyFunction\"\n" +
						"pages = REPARTITION SORT pages\n" +
						"ranks = MAP pages BY \"PageToRankFunction\"\n" +
						"ranks = CAST SORT ranks BY \"RankKeyFunction\"\n" +
						"\n" +
						"REPEAT 10 TIMES\n" +
						"    updates = JOIN pages AND ranks WITH \"PageRankJoiner\" RESULT \"Rank\" BY \"RankKeyFunction\"\n" +
						"    ranks = SORT REDUCE REPARTITION REDUCE updates TYPE \"Rank\"\n" +
						"                 WITH \"RankAccumulatorReducer\"\n" +
						"                 BY \"RankKeyFunction\"\n" +
						"                 ACCUMULATING BY \"RankAccumulator\"\n" +
						"                 EXTRACTING BY \"RankAccumulatorKeyFunction\"\n" +
						"    ranks = CAST SORT ranks BY \"RankKeyFunction\"\n" +
						"END\n" +
						"\n" +
						"WRITE ranks INTO \"result\"\n"
		);
		query.evaluate(new EvaluationContext(graph));

//		Dataset<Rank> newRanks = sort_Reduce_Repartition_Reduce(updates, new RankAccumulatorReducer(),
//				Long.class, new RankKeyFunction(), new LongComparator(),
//				RankAccumulator.class, new RankAccumulatorKeyFunction(),
//				Rank.class);

//		USE "io.datakernel.dataflow.stream.PageRankTest$"
//
//		pages = DATASET "items" -> "Page"
//		pages = CAST SORT pages BY "PageKeyFunction" COMPARING WITH "PageToRankFunction"
//		pages = REPARTITION SORT pages
//		ranks = MAP pages BY "PageToRankFunction"
//		ranks = CAST SORT ranks BY "RankKeyFunction" /* COMPARING WITH "LongComparator" */
//
//		REPEAT 10 TIMES
//			updates = JOIN pages AND ranks WITH "PageRankJoiner" BY "RankKeyFunction"
//			ranks = SORT REDUCE REPARTITION REDUCE updates
//						WITH "RankAccumulatorReducer"
//						BY "RankKeyFunction"
//						/* COMPARING WITH "LongComparator" */
//			ranks = CAST SORT ranks "RankKeyFunction" /* COMPARING WITH "LongComparator" */
//		END
//
//		WRITE ranks INTO "result"

//		SortedDataset<Long, Page> pages = repartition_Sort(sortedDatasetOfList("items",
//				Page.class, Long.class, new PageKeyFunction(), new LongComparator()));

//		SortedDataset<Long, Rank> ranks = castToSorted(map(pages, new PageToRankFunction(), Rank.class),
//				Long.class, new RankKeyFunction(), new LongComparator());
//
//		for (int i = 0; i < 10; i++) {
//			Dataset<Rank> updates = join(pages, ranks, new PageRankJoiner(), Rank.class, new RankKeyFunction());
//
//			Dataset<Rank> newRanks = sort_Reduce_Repartition_Reduce(updates, new RankAccumulatorReducer(),
//					Long.class, new RankKeyFunction(), new LongComparator(),
//					RankAccumulator.class, new RankAccumulatorKeyFunction(),
//					Rank.class);
//
//			ranks =  castToSorted(newRanks, Long.class, new RankKeyFunction(), new LongComparator());
//		}
//
//		SortedDataset<Long, Rank> pageRanks = pageRank(pages);

//		DatasetListConsumer<?> consumerNode = listConsumer(pageRanks, "result");
//		consumerNode.compileInto(graph);

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		List<Rank> result = new ArrayList<>();
		result.addAll(result1.getList());
		result.addAll(result2.getList());
		result.sort(Comparator.comparingLong(rank -> rank.pageId));

		assertEquals(asList(new Rank(1, 1.7861), new Rank(2, 0.6069), new Rank(3, 0.6069)), result);
	}
}
