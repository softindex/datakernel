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

import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.dataset.LocallySortedDataset;
import io.datakernel.dataflow.dataset.SortedDataset;
import io.datakernel.dataflow.dataset.impl.DatasetListConsumer;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.datakernel.dataflow.server.*;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public final class DataflowTest {
	private static DataflowServer server1;
	private static DataflowServer server2;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testForward() throws Exception {
		DataflowSerialization serialization = DataflowSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new));

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		DataflowClient client = new DataflowClient(serialization);

		DataflowEnvironment environment = DataflowEnvironment.create()
				.setInstance(DataflowSerialization.class, serialization);
		DataflowEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(3),
						new TestItem(5)))
				.with("result", result1);
		DataflowEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(2),
						new TestItem(4),
						new TestItem(6)))
				.with("result", result2);

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();
		server1 = new DataflowServer(Eventloop.getCurrentEventloop(), environment1).withListenAddress(address1);
		server2 = new DataflowServer(Eventloop.getCurrentEventloop(), environment2).withListenAddress(address2);
		server1.listen();
		server2.listen();

		DataflowGraph graph = new DataflowGraph(client, serialization, asList(new Partition(address1), new Partition(address2)));

		Dataset<TestItem> items = datasetOfList("items", TestItem.class);
		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);
		await(cleanUp(graph.execute()));

		assertEquals(asList(new TestItem(1), new TestItem(3), new TestItem(5)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result2.getList());
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		DataflowSerialization serialization = DataflowSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new));

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		DataflowClient client = new DataflowClient(serialization);

		DataflowEnvironment environment = DataflowEnvironment.create()
				.setInstance(DataflowSerialization.class, serialization)
				.setInstance(DataflowClient.class, client);
		DataflowEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5),
						new TestItem(6)))
				.with("result", result1);
		DataflowEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(6)))
				.with("result", result2);

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();
		server1 = new DataflowServer(Eventloop.getCurrentEventloop(), environment1).withListenAddress(address1);
		server2 = new DataflowServer(Eventloop.getCurrentEventloop(), environment2).withListenAddress(address2);
		server1.listen();
		server2.listen();

		DataflowGraph graph = new DataflowGraph(client, serialization, asList(new Partition(address1), new Partition(address2)));

		SortedDataset<Long, TestItem> items = repartition_Sort(sortedDatasetOfList("items",
				TestItem.class, Long.class, new TestKeyFunction(), new TestComparator()));
		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);
		await(cleanUp(graph.execute()));

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(1), new TestItem(1), new TestItem(3), new TestItem(5)), result2.getList());
	}

	@Test
	public void testFilter() throws Exception {
		DataflowSerialization serialization = DataflowSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new))
				.withCodec(TestPredicate.class, ofObject(TestPredicate::new));

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		DataflowClient client = new DataflowClient(serialization);

		DataflowEnvironment environment = DataflowEnvironment.create()
				.setInstance(DataflowSerialization.class, serialization)
				.setInstance(DataflowClient.class, client)
				.setInstance(StreamSorterStorageFactory.class, FACTORY_STUB);
		DataflowEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(6),
						new TestItem(4),
						new TestItem(2),
						new TestItem(3),
						new TestItem(1)))
				.with("result", result1);
		DataflowEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(7),
						new TestItem(7),
						new TestItem(8),
						new TestItem(2),
						new TestItem(5)))
				.with("result", result2);

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();
		server1 = new DataflowServer(Eventloop.getCurrentEventloop(), environment1).withListenAddress(address1);
		server2 = new DataflowServer(Eventloop.getCurrentEventloop(), environment2).withListenAddress(address2);
		server1.listen();
		server2.listen();

		DataflowGraph graph = new DataflowGraph(client, serialization, asList(new Partition(address1), new Partition(address2)));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());
		DatasetListConsumer<?> consumerNode = listConsumer(sortedDataset, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);
		await(cleanUp(graph.execute()));

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(8)), result2.getList());
	}

	@Test
	public void testCollector() throws Exception {
		DataflowSerialization serialization = DataflowSerialization.create()
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new))
				.withCodec(TestPredicate.class, ofObject(TestPredicate::new));

		StreamConsumerToList<TestItem> resultConsumer = StreamConsumerToList.create();

		DataflowClient client = new DataflowClient(serialization);

		DataflowEnvironment environment = DataflowEnvironment.create()
				.setInstance(DataflowSerialization.class, serialization)
				.setInstance(DataflowClient.class, client)
				.setInstance(StreamSorterStorageFactory.class, FACTORY_STUB);
		DataflowEnvironment environment1 = environment.extend()
				.with("items", asList(
						new TestItem(1),
						new TestItem(2),
						new TestItem(3),
						new TestItem(4),
						new TestItem(5)));
		DataflowEnvironment environment2 = environment.extend()
				.with("items", asList(
						new TestItem(6),
						new TestItem(7),
						new TestItem(8),
						new TestItem(9),
						new TestItem(10)
				));

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();
		server1 = new DataflowServer(Eventloop.getCurrentEventloop(), environment1).withListenAddress(address1);
		server2 = new DataflowServer(Eventloop.getCurrentEventloop(), environment2).withListenAddress(address2);
		server1.listen();
		server2.listen();

		DataflowGraph graph = new DataflowGraph(client, serialization, asList(new Partition(address1), new Partition(address2)));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class), new TestPredicate());
		LocallySortedDataset<Long, TestItem> sortedDataset = localSort(filterDataset, long.class, new TestKeyFunction(), new TestComparator());
		Collector<TestItem> collector = new Collector<>(sortedDataset, client);
		StreamSupplier<TestItem> resultSupplier = collector.compile(graph);
		resultSupplier.streamTo(resultConsumer);

		System.out.println(graph);
		await(cleanUp(graph.execute()));

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(8), new TestItem(10)), resultConsumer.getList());
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
		return promise.whenComplete(() -> {
			server1.close();
			server2.close();
		});
	}

	static InetSocketAddress getFreeListenAddress() throws UnknownHostException {
		return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), getFreePort());
	}
}
