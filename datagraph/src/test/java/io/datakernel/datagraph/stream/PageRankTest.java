/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.datagraph.stream;

import io.datakernel.async.Stages;
import io.datakernel.datagraph.dataset.Dataset;
import io.datakernel.datagraph.dataset.SortedDataset;
import io.datakernel.datagraph.dataset.impl.DatasetListConsumer;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.Partition;
import io.datakernel.datagraph.helper.StreamMergeSorterStorageStub;
import io.datakernel.datagraph.server.DatagraphClient;
import io.datakernel.datagraph.server.DatagraphEnvironment;
import io.datakernel.datagraph.server.DatagraphSerialization;
import io.datakernel.datagraph.server.DatagraphServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.processor.StreamJoin;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamSorterStorage;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;

import static io.datakernel.async.Stages.assertComplete;
import static io.datakernel.datagraph.dataset.Datasets.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class PageRankTest {

	public static final class Page {
		@Serialize(order = 0)
		public final long pageId;
		@Serialize(order = 1)
		public final long[] links;

		public Page(@Deserialize("pageId") long pageId, @Deserialize("links") long[] links) {
			this.pageId = pageId;
			this.links = links;
		}

		public void disperse(Rank rank, StreamDataReceiver<Rank> callback) {
			for (long link : links) {
				Rank newRank = new Rank(link, rank.value / links.length);
				callback.onData(newRank);
			}
		}

		public static final Function<Page, Long> KEY_FUNCTION = page -> page.pageId;

		@Override
		public String toString() {
			return "Page{" +
					"pageId=" + pageId +
					", links=" + Arrays.toString(links) +
					'}';
		}
	}

	public static final class Rank {
		@Serialize(order = 0)
		public final long pageId;
		@Serialize(order = 1)
		public final double value;

		public Rank(@Deserialize("pageId") long pageId, @Deserialize("value") double value) {
			this.pageId = pageId;
			this.value = value;
		}

		public static final Function<Rank, Long> KEY_FUNCTION = rank -> rank.pageId;

		@Override
		public String toString() {
			return "Rank{" +
					"pageId=" + pageId +
					", value=" + value +
					'}';
		}

		@SuppressWarnings({"SimplifiableIfStatement", "EqualsWhichDoesntCheckParameterClass"})
		@Override
		public boolean equals(Object o) {
			Rank rank = (Rank) o;
			if (pageId != rank.pageId) return false;
			return Math.abs(rank.value - value) < 0.001;
		}
	}

	public static final class RankAccumulator {
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

		public static final Function<RankAccumulator, Long> KEY_FUNCTION = rankAccumulator -> rankAccumulator.pageId;

		@Override
		public String toString() {
			return "RankAccumulator{" +
					"pageId=" + pageId +
					", accumulatedRank=" + accumulatedRank +
					'}';
		}
	}

	private static final class RankAccumulatorReducer extends StreamReducers.ReducerToResult<Long, Rank, Rank, RankAccumulator> {
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

	public static SortedDataset<Long, Rank> pageRankIteration(SortedDataset<Long, Page> pages, SortedDataset<Long, Rank> ranks) {
		Dataset<Rank> updates = join(pages, ranks,
				new StreamJoin.InnerJoiner<Long, Page, Rank, Rank>() {
					@Override
					public void onInnerJoin(Long key, Page page, Rank rank, StreamDataReceiver<Rank> output) {
						page.disperse(rank, output);
					}
				},
				Rank.class, Rank.KEY_FUNCTION);

		Dataset<Rank> newRanks = sort_Reduce_Repartition_Reduce(updates, new RankAccumulatorReducer(),
				Long.class, Rank.KEY_FUNCTION, Long::compareTo,
				RankAccumulator.class, RankAccumulator.KEY_FUNCTION,
				Rank.class);

		return castToSorted(newRanks, Long.class, Rank.KEY_FUNCTION, Long::compareTo);
	}

	public static SortedDataset<Long, Rank> pageRank(SortedDataset<Long, Page> pages) {
		SortedDataset<Long, Rank> ranks = castToSorted(map(pages,
				new StreamMap.MapperProjection<Page, Rank>() {
					@Override
					public Rank apply(Page page) {
						return new Rank(page.pageId, 1.0);
					}
				},
				Rank.class), Long.class, Rank.KEY_FUNCTION, Comparator.naturalOrder());

		for (int i = 0; i < 10; i++) {
			ranks = pageRankIteration(pages, ranks);
		}

		return ranks;
	}

	@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
	@Ignore // TODO(vmykhalko)
	@Test
	public void test2() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create();
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1571);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1572);

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		StreamConsumerToList<Rank> result1 = StreamConsumerToList.create();
		StreamConsumerToList<Rank> result2 = StreamConsumerToList.create();

		DatagraphClient client = new DatagraphClient(eventloop, serialization);
		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamSorterStorage.class, new StreamMergeSorterStorageStub(eventloop));
		DatagraphEnvironment environment1 = environment.extend()
				.set("items", asList(new Page(1, new long[]{1, 2, 3}), new Page(3, new long[]{1})))
				.set("result", result1);
		DatagraphEnvironment environment2 = environment.extend()
				.set("items", asList(new Page(2, new long[]{1})))
				.set("result", result2);

		DatagraphServer server1 = new DatagraphServer(eventloop, environment1).withListenAddress(address1);
		DatagraphServer server2 = new DatagraphServer(eventloop, environment2).withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);

		DataGraph graph = new DataGraph(serialization,
				asList(partition1, partition2));

		SortedDataset<Long, Page> pages = repartition_Sort(sortedDatasetOfList("items",
				Page.class, Long.class, Page.KEY_FUNCTION, Comparator.naturalOrder()));

		SortedDataset<Long, Rank> pageRanks = pageRank(pages);

		DatasetListConsumer<?> consumerNode = listConsumer(pageRanks, "result");
		consumerNode.compileInto(graph);

		server1.listen();
		server2.listen();

		Stages.all(result1.getResult().thenApply($ -> null), result2.getResult().thenApply($ -> null))
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				}));

		graph.execute();

		eventloop.run();

		assertEquals(asList(new Rank(2, 0.6069)), result1.getList());
		assertEquals(asList(new Rank(1, 1.7861), new Rank(3, 0.6069)), result2.getList());
	}

}
