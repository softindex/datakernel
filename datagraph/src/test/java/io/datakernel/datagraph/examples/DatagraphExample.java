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

package io.datakernel.datagraph.examples;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import com.google.common.net.InetAddresses;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.datagraph.dataset.Dataset;
import io.datakernel.datagraph.dataset.LocallySortedDataset;
import io.datakernel.datagraph.dataset.impl.DatasetListConsumer;
import io.datakernel.datagraph.graph.DataGraph;
import io.datakernel.datagraph.graph.Partition;
import io.datakernel.datagraph.graph.RemotePartition;
import io.datakernel.datagraph.server.DatagraphClient;
import io.datakernel.datagraph.server.DatagraphEnvironment;
import io.datakernel.datagraph.server.DatagraphSerialization;
import io.datakernel.datagraph.server.DatagraphServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.processor.StreamMergeSorterStorage;
import io.datakernel.stream.processor.StreamMergeSorterStorageStub;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.datagraph.dataset.Datasets.*;
import static java.util.Arrays.asList;

/*
Example of creating two datagraph servers that process two data streams independently.
We construct the following processing chain: TestItem producer -> filter -> sorter -> TestItem consumer.
Filter is set to filter out odd numbers.
So we end up with two parallel streams that output sorted TestItem's with even values.
 */
public class DatagraphExample {
	private static final int PORT1 = 1511;
	private static final int PORT2 = 1512;
	private static final InetAddress LOCALHOST = InetAddresses.forString("127.0.0.1");

	public static void main(String[] args) throws IOException {
		DatagraphSerialization serialization = new DatagraphSerialization();

		NioEventloop eventloop = new NioEventloop();
		DatagraphClient client = new DatagraphClient(eventloop, serialization);

		// Streams for consuming the result
		StreamConsumers.ToList<TestItem> result1 = new StreamConsumers.ToList<>(eventloop);
		StreamConsumers.ToList<TestItem> result2 = new StreamConsumers.ToList<>(eventloop);

		// Parent environment common for two servers
		DatagraphEnvironment environment = DatagraphEnvironment.create()
				.setInstance(DatagraphSerialization.class, serialization)
				.setInstance(DatagraphClient.class, client)
				.setInstance(StreamMergeSorterStorage.class, new StreamMergeSorterStorageStub(eventloop));

		// Specific environments for two servers with test items and result streams
		List<TestItem> testItems1 = asList(new TestItem(6), new TestItem(4), new TestItem(2),
				new TestItem(3), new TestItem(1));
		DatagraphEnvironment environment1 = environment.extend()
				.set("items", testItems1)
				.set("result", result1);
		List<TestItem> testItems2 = asList(new TestItem(7), new TestItem(7), new TestItem(8),
				new TestItem(2), new TestItem(5));
		DatagraphEnvironment environment2 = environment.extend()
				.set("items", testItems2)
				.set("result", result2);
		System.out.println("First stream input: " + testItems1);
		System.out.println("Second stream input: " + testItems2);

		// Set up servers
		InetSocketAddress address1 = new InetSocketAddress(LOCALHOST, PORT1);
		InetSocketAddress address2 = new InetSocketAddress(LOCALHOST, PORT2);
		final DatagraphServer server1 = new DatagraphServer(eventloop, environment1)
				.setListenAddress(address1);
		final DatagraphServer server2 = new DatagraphServer(eventloop, environment2)
				.setListenAddress(address2);

		// Set up partitions
		Partition partition1 = new RemotePartition(client, address1);
		Partition partition2 = new RemotePartition(client, address2);

		// Define a task graph for partitions
		DataGraph graph = new DataGraph(serialization, asList(partition1, partition2));

		// Dataset that filters out odd numbers
		Dataset<TestItem> filterDataset = filter(datasetOfList("items", TestItem.class),
				new Predicate<TestItem>() {
					@Override
					public boolean apply(TestItem input) {
						return input.value % 2 == 0;
					}
				});

		// Dataset that sorts each parallel stream independently from others
		LocallySortedDataset<Long, TestItem> sortedDataset =
				localSort(filterDataset, long.class, new TestItem.KeyFunction(), Ordering.<Long>natural());

		// Consumer dataset for result
		DatasetListConsumer<?> consumerNode = listConsumer(sortedDataset, "result");

		// Compile the defined processing chain into a graph
		consumerNode.compileInto(graph);

		// Start datagraph servers
		server1.listen();
		server2.listen();

		// Add callbacks that close server once result streaming is done
		result1.setCompletionCallback(getServerClosingCallback(server1));
		result2.setCompletionCallback(getServerClosingCallback(server2));

		// Execute task graph
		graph.execute();

		eventloop.run();

		System.out.println("First stream output: " + result1.getList());
		System.out.println("Second stream output: " + result2.getList());
	}

	// Returns a callback that closes server
	private static CompletionCallback getServerClosingCallback(final DatagraphServer server) {
		return new SimpleCompletionCallback() {
			@Override
			protected void onCompleteOrException() {
				server.close();
			}
		};
	}

	// Simple class that holds a long value field, marked for serialization
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

		// Function that extracts the key value
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
}
