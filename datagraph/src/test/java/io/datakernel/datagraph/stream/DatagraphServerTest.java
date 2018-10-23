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

package io.datakernel.datagraph.stream;

import io.datakernel.datagraph.dataset.Dataset;
import io.datakernel.datagraph.dataset.LocallySortedDataset;
import io.datakernel.datagraph.dataset.SortedDataset;
import io.datakernel.datagraph.dataset.impl.DatasetListConsumer;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.Partition;
import io.datakernel.datagraph.helper.StreamMergeSorterStorageStub;
import io.datakernel.datagraph.server.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.ActiveStagesRule;
import io.datakernel.stream.processor.StreamSorterStorage;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.datagraph.dataset.Datasets.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DatagraphServerTest {
	@Rule
	public ActiveStagesRule activeStagesRule = new ActiveStagesRule();

	public static final class TestItem {
		@Serialize(order = 0)
		public final long value;

		public TestItem(@Deserialize("value") long value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "TestItem{value=" + value + '}';
		}

		public static class KeyFunction implements Function<TestItem, Long> {
			@Override
			public Long apply(TestItem item) {
				return item.value;
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestItem testItem = (TestItem) o;

			return value == testItem.value;

		}

		@Override
		public int hashCode() {
			return (int) (value ^ (value >>> 32));
		}
	}

	private static int testPort = 1511;

	@Test
	public void testForward() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create();
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization);
		DatagraphEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(3),
						new TestItem(5)))
				.with("result", result1);
		DatagraphEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(2),
						new TestItem(4),
						new TestItem(6)))
				.with("result", result2);

		DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		DatagraphClient client = new DatagraphClient(serialization);
		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		Dataset<TestItem> items = datasetOfList("items", TestItem.class);

		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);

		server1.listen();
		server2.listen();

		result1.getResult()
				.whenComplete(($, err) -> server1.close())
				.whenComplete(assertComplete());

		result2.getResult()
				.whenComplete(($, err) -> server2.close())
				.whenComplete(assertComplete());

		graph.execute();

		eventloop.run();

		assertEquals(asList(new TestItem(1), new TestItem(3), new TestItem(5)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result2.getList());
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create();
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		DatagraphClient client = new DatagraphClient(serialization);

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client);
		DatagraphEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5),
						new TestItem(6)))
				.with("result", result1);
		DatagraphEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(6)))
				.with("result", result2);
		DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		DataGraph graph = new DataGraph(serialization,
				asList(partition1, partition2));

		SortedDataset<Long, TestItem> items = repartition_Sort(sortedDatasetOfList("items",
				TestItem.class, Long.class, new TestItem.KeyFunction(), new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return o1.compareTo(o2);
					}
				}));

		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);

		server1.listen();
		server2.listen();

		result1.getResult()
				.whenComplete(($, err) -> server1.close())
				.whenComplete(assertComplete());

		result2.getResult()
				.whenComplete(($, err) -> server2.close())
				.whenComplete(assertComplete());

		graph.execute();

		eventloop.run();

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(1), new TestItem(1), new TestItem(3), new TestItem(5)), result2.getList());
	}

	@Test
	public void testFilter() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create();
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DatagraphClient client = new DatagraphClient(serialization);
		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamSorterStorage.class, new StreamMergeSorterStorageStub(eventloop));
		DatagraphEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(6),
						new TestItem(4),
						new TestItem(2),
						new TestItem(3),
						new TestItem(1)))
				.with("result", result1);
		DatagraphEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(7),
						new TestItem(7),
						new TestItem(8),
						new TestItem(2),
						new TestItem(5)))
				.with("result", result2);

		DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class),
				new Predicate<TestItem>() {
					@Override
					public boolean test(TestItem input) {
						return input.value % 2 == 0;
					}
				});

		LocallySortedDataset<Long, TestItem> sortedDataset =
				localSort(filterDataset, long.class, new TestItem.KeyFunction(), new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return o1.compareTo(o2);
					}
				});

		DatasetListConsumer<?> consumerNode = listConsumer(sortedDataset, "result");

		consumerNode.compileInto(graph);

		System.out.println("Graph: ");
		System.out.println(graph);

		server1.listen();
		server2.listen();

		result1.getResult()
				.whenComplete(($, err) -> server1.close())
				.whenComplete(assertComplete());

		result2.getResult()
				.whenComplete(($, err) -> server2.close())
				.whenComplete(assertComplete());

		graph.execute();

		eventloop.run();

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(8)), result2.getList());
	}

	@Test
	public void testCollector() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create();
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DatagraphClient client = new DatagraphClient(serialization);
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamSorterStorage.class, new StreamMergeSorterStorageStub(eventloop));
		DatagraphEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)));
		DatagraphEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(6),
						new TestItem(7),
						new TestItem(8),
						new TestItem(9),
						new TestItem(10)
				));

		DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class),
				new Predicate<TestItem>() {
					@Override
					public boolean test(TestItem input) {
						return input.value % 2 == 0;
					}
				});

		LocallySortedDataset<Long, TestItem> sortedDataset =
				localSort(filterDataset, long.class, new TestItem.KeyFunction(), new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return o1.compareTo(o2);
					}
				});

		server1.listen();
		server2.listen();

		Collector<TestItem> collector = new Collector<>(sortedDataset, TestItem.class, client, eventloop);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		System.out.println("Graph: ");
		System.out.println(graph);

		resultSupplier.streamTo(resultConsumer)
				.whenComplete(($, err) -> {
					server1.close();
					server2.close();
				})
				.whenComplete(assertComplete());

		graph.execute();

		eventloop.run();

		assertEquals(asList(
				new TestItem(2),
				new TestItem(4),
				new TestItem(6),
				new TestItem(8),
				new TestItem(10)),
				resultConsumer.getList());
	}
}
