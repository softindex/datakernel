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

import io.datakernel.async.Promise;
import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.dataset.LocallySortedDataset;
import io.datakernel.dataflow.dataset.SortedDataset;
import io.datakernel.dataflow.dataset.impl.DatasetListConsumer;
import io.datakernel.dataflow.graph.DataGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.helper.StreamMergeSorterStorageStub;
import io.datakernel.dataflow.server.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.StreamSorterStorage;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
@RunWith(DatakernelRunner.class)
public final class DatagraphServerTest {

	private static int testPort = 1511;
	private static DatagraphServer server1;
	private static DatagraphServer server2;

	@Test
	public void testForward() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new));
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

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

		server1 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment1)
				.withListenAddress(address1);
		server2 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment2)
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
		graph.execute();

		List<TestItem> list1 = await(cleanUp(result1.getResult()));
		List<TestItem> list2 = await(result2.getResult());

		assertEquals(asList(new TestItem(1), new TestItem(3), new TestItem(5)), list1);
		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), list2);
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new));
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

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
		server1 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment1)
				.withListenAddress(address1);
		server2 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		DataGraph graph = new DataGraph(serialization,
				asList(partition1, partition2));

		SortedDataset<Long, TestItem> items = repartition_Sort(sortedDatasetOfList("items",
				TestItem.class, Long.class, new TestKeyFunction(), new TestComparator()));

		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);

		server1.listen();
		server2.listen();
		graph.execute();

		List<TestItem> list1 = await(cleanUp(result1.getResult()));
		List<TestItem> list2 = await(result2.getResult());

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(6)), list1);
		assertEquals(asList(new TestItem(1), new TestItem(1), new TestItem(3), new TestItem(5)), list2);
	}

	@Test
	public void testFilter() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new))
				.withCodec(TestPredicate.class, ofObject(TestPredicate::new));
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

		DatagraphClient client = new DatagraphClient(serialization);
		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamSorterStorage.class, new StreamMergeSorterStorageStub<>(Eventloop.getCurrentEventloop()));
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

		server1 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment1)
				.withListenAddress(address1);
		server2 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class), new TestPredicate());

		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());

		DatasetListConsumer<?> consumerNode = listConsumer(sortedDataset, "result");

		consumerNode.compileInto(graph);

		System.out.println("Graph: ");
		System.out.println(graph);

		server1.listen();
		server2.listen();
		graph.execute();

		List<TestItem> list1 = await(cleanUp(result1.getResult()));
		List<TestItem> list2 = await(result2.getResult());

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), list1);
		assertEquals(asList(new TestItem(2), new TestItem(8)), list2);
	}

	@Test
	public void testCollector() throws Exception {
		DatagraphSerialization serialization = DatagraphSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new))
				.withCodec(TestPredicate.class, ofObject(TestPredicate::new));
		InetSocketAddress address1 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);
		InetSocketAddress address2 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), testPort++);

		DatagraphClient client = new DatagraphClient(serialization);
		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamSorterStorage.class, new StreamMergeSorterStorageStub<>(Eventloop.getCurrentEventloop()));
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

		server1 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment1)
				.withListenAddress(address1);
		server2 = new DatagraphServer(Eventloop.getCurrentEventloop(), environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class), new TestPredicate());

		LocallySortedDataset<Long, TestItem> sortedDataset =
				localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());

		server1.listen();
		server2.listen();

		Collector<TestItem> collector = new Collector<>(sortedDataset, TestItem.class, client);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);

		System.out.println("Graph: ");
		System.out.println(graph);
		graph.execute();

		await(cleanUp(resultSupplier.streamTo(resultConsumer)));
		List<TestItem> list = await(resultConsumer.getResult());
		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(8), new TestItem(10)), list);
	}

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

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			TestItem other = (TestItem) o;
			return value == other.value;
		}

		@Override
		public int hashCode() {
			return (int) (value ^ (value >>> 32));
		}
	}

	private static class TestComparator implements Comparator<Long> {
		@Override
		public int compare(Long o1, Long o2) {
			return o1.compareTo(o2);
		}
	}

	public static class TestKeyFunction implements Function<TestItem, Long> {
		@Override
		public Long apply(TestItem item) {
			return item.value;
		}
	}

	private static class TestPredicate implements Predicate<TestItem> {
		@Override
		public boolean test(TestItem input) {
			return input.value % 2 == 0;
		}
	}

	private static <T> Promise<T> cleanUp(Promise<T> promise) {
		return promise
				.acceptEx(($, e) -> {
					server1.close();
					server2.close();
				});
	}
}
