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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import com.google.common.net.InetAddresses;
import io.datakernel.async.AssertingCompletionCallback;
import io.datakernel.async.IgnoreCompletionCallback;
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
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamMergeSorterStorage;
import org.junit.Test;

import java.net.InetSocketAddress;

import static io.datakernel.datagraph.dataset.Datasets.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DatagraphServerTest {

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

	@Test
	public void testForward() throws Exception {
		DatagraphSerialization serialization = new DatagraphSerialization();
		InetSocketAddress address1 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1511);
		InetSocketAddress address2 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1512);

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		StreamConsumers.ToList<TestItem> result1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<TestItem> result2 = new StreamConsumers.ToList<>(eventloop);

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization);
		DatagraphEnvironment environment1 = environment.extend()
				.set("items", asList(new TestItem(1), new TestItem(3), new TestItem(5)))
				.set("result", result1);
		DatagraphEnvironment environment2 = environment.extend()
				.set("items", asList(new TestItem(2), new TestItem(4), new TestItem(6)))
				.set("result", result2);

		final DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		final DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		DatagraphClient client = new DatagraphClient(eventloop, serialization);
		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		final DataGraph graph = new DataGraph(serialization,
				asList(partition1, partition2));

		Dataset<TestItem> items = datasetOfList("items", TestItem.class);

		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);

		server1.listen();
		server2.listen();

		result1.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server1.close(IgnoreCompletionCallback.create());
			}
		});

		result2.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server2.close(IgnoreCompletionCallback.create());
			}
		});

		graph.execute();

		eventloop.run();

		assertEquals(asList(new TestItem(1), new TestItem(3), new TestItem(5)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result2.getList());
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		DatagraphSerialization serialization = new DatagraphSerialization();
		InetSocketAddress address1 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1511);
		InetSocketAddress address2 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1512);

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		StreamConsumers.ToList<TestItem> result1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<TestItem> result2 = new StreamConsumers.ToList<>(eventloop);

		DatagraphClient client = new DatagraphClient(eventloop, serialization);

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client);
		DatagraphEnvironment environment1 = environment.extend()
				.set("items", asList(new TestItem(1), new TestItem(2), new TestItem(3), new TestItem(4), new TestItem(5), new TestItem(6)))
				.set("result", result1);
		DatagraphEnvironment environment2 = environment.extend()
				.set("items", asList(new TestItem(1), new TestItem(6)))
				.set("result", result2);
		final DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		final DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		final DataGraph graph = new DataGraph(serialization,
				asList(partition1, partition2));

		SortedDataset<Long, TestItem> items = repartition_Sort(sortedDatasetOfList("items",
				TestItem.class, Long.class, new TestItem.KeyFunction(), Ordering.<Long>natural()));

		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);

		server1.listen();
		server2.listen();

		result1.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server1.close(IgnoreCompletionCallback.create());
			}
		});

		result2.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server2.close(IgnoreCompletionCallback.create());
			}
		});

		graph.execute();

		eventloop.run();

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(1), new TestItem(1), new TestItem(3), new TestItem(5)), result2.getList());
	}

	@Test
	public void testFilter() throws Exception {
		DatagraphSerialization serialization = new DatagraphSerialization();
		InetSocketAddress address1 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1511);
		InetSocketAddress address2 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1512);

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		DatagraphClient client = new DatagraphClient(eventloop, serialization);
		StreamConsumers.ToList<TestItem> result1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<TestItem> result2 = new StreamConsumers.ToList<>(eventloop);

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamMergeSorterStorage.class, new StreamMergeSorterStorageStub(eventloop));
		DatagraphEnvironment environment1 = environment.extend()
				.set("items", asList(new TestItem(6), new TestItem(4), new TestItem(2),
						new TestItem(3), new TestItem(1)))
				.set("result", result1);
		DatagraphEnvironment environment2 = environment.extend()
				.set("items", asList(new TestItem(7), new TestItem(7), new TestItem(8),
						new TestItem(2), new TestItem(5)))
				.set("result", result2);

		final DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		final DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		final DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class),
				new Predicate<TestItem>() {
					@Override
					public boolean apply(TestItem input) {
						return input.value % 2 == 0;
					}
				});

		LocallySortedDataset<Long, TestItem> sortedDataset =
				localSort(filterDataset, long.class, new TestItem.KeyFunction(), Ordering.<Long>natural());

		DatasetListConsumer<?> consumerNode = listConsumer(sortedDataset, "result");

		consumerNode.compileInto(graph);

		System.out.println("Graph: ");
		System.out.println(graph);

		server1.listen();
		server2.listen();

		result1.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server1.close(IgnoreCompletionCallback.create());
			}
		});

		result2.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server2.close(IgnoreCompletionCallback.create());
			}
		});

		graph.execute();

		eventloop.run();

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6)), result1.getList());
		assertEquals(asList(new TestItem(2), new TestItem(8)), result2.getList());
	}

	@Test
	public void testCollector() throws Exception {
		DatagraphSerialization serialization = new DatagraphSerialization();
		InetSocketAddress address1 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1511);
		InetSocketAddress address2 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), 1512);

		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		DatagraphClient client = new DatagraphClient(eventloop, serialization);
		final StreamConsumers.ToList<TestItem> resultConsumer = new StreamConsumers.ToList<>(eventloop);

		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamMergeSorterStorage.class, new StreamMergeSorterStorageStub(eventloop));
		DatagraphEnvironment environment1 = environment.extend()
				.set("items", asList(new TestItem(1), new TestItem(2), new TestItem(3),
						new TestItem(4), new TestItem(5)));
		DatagraphEnvironment environment2 = environment.extend()
				.set("items", asList(new TestItem(6), new TestItem(7), new TestItem(8),
						new TestItem(9), new TestItem(10)));

		final DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.withListenAddress(address1);
		final DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.withListenAddress(address2);

		Partition partition1 = new Partition(client, address1);
		Partition partition2 = new Partition(client, address2);
		final DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class),
				new Predicate<TestItem>() {
					@Override
					public boolean apply(TestItem input) {
						return input.value % 2 == 0;
					}
				});

		LocallySortedDataset<Long, TestItem> sortedDataset =
				localSort(filterDataset, long.class, new TestItem.KeyFunction(), Ordering.<Long>natural());

		System.out.println("Graph: ");
		System.out.println(graph);

		server1.listen();
		server2.listen();

		Collector<TestItem> collector = new Collector<>(sortedDataset, TestItem.class, client, eventloop);
		StreamProducer<TestItem> resultProducer = collector.compile(graph);
		resultProducer.streamTo(resultConsumer);

		resultConsumer.setCompletionCallback(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server1.close(IgnoreCompletionCallback.create());
				server2.close(IgnoreCompletionCallback.create());
			}
		});

		graph.execute();

		eventloop.run();

		assertEquals(asList(new TestItem(2), new TestItem(4), new TestItem(6), new TestItem(8), new TestItem(10)),
				resultConsumer.getList());
	}
}
