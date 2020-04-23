package io.datakernel.dataflow.stream;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.binary.ByteBufsCodec;
import io.datakernel.dataflow.dataset.SortedDataset;
import io.datakernel.dataflow.dataset.impl.DatasetListConsumer;
import io.datakernel.dataflow.di.BinarySerializersModule;
import io.datakernel.dataflow.di.CodecsModule.Subtypes;
import io.datakernel.dataflow.di.DataflowModule;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.dataflow.server.DataflowServer;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphResponse;
import io.datakernel.dataflow.stream.DataflowTest.TestComparator;
import io.datakernel.dataflow.stream.DataflowTest.TestItem;
import io.datakernel.dataflow.stream.DataflowTest.TestKeyFunction;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.dataflow.di.EnvironmentModule.slot;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ReducerDeadlockTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

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

	@Test
	public void test() throws IOException {

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		Module common = ModuleBuilder.create()
				.install(DataflowModule.create())
				.bind(Executor.class).toInstance(executor)
				.bind(Eventloop.class).toInstance(Eventloop.getCurrentEventloop())

				.scan(new Object() {

					@Provides
					DataflowServer server(Eventloop eventloop, ByteBufsCodec<DatagraphCommand, DatagraphResponse> codec, BinarySerializersModule.BinarySerializers serializers, Injector environment) {
						return new DataflowServer(eventloop, codec, serializers, environment);
					}

					@Provides
					DataflowClient client(Executor executor, ByteBufsCodec<DatagraphResponse, DatagraphCommand> codec, BinarySerializersModule.BinarySerializers serializers) throws IOException {
						return new DataflowClient(executor, codec, serializers)
								.withSecondaryBufferPath(temporaryFolder.newFolder().toPath());
					}

					@Provides
					DataflowGraph graph(DataflowClient client, @Subtypes StructuredCodec<Node> nodeCodec) {
						return new DataflowGraph(client, asList(new Partition(address1), new Partition(address2)), nodeCodec);
					}
				})

				.bind(new Key<StructuredCodec<TestComparator>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<StructuredCodec<TestKeyFunction>>() {}).toInstance(ofObject(TestKeyFunction::new))

				.build();

		StreamConsumerToList<TestItem> result1 = StreamConsumerToList.create();
		StreamConsumerToList<TestItem> result2 = StreamConsumerToList.create();

		List<TestItem> list1 = new ArrayList<>(20000);
		for (int i = 0; i < 20000; i++) {
			list1.add(new TestItem(i * 2 + 2));
		}

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(list1)
				.bind(slot("result")).toInstance(result1)
				.build();

		List<TestItem> list2 = new ArrayList<>(20000);
		for (int i = 0; i < 20000; i++) {
			list2.add(new TestItem(i * 2 + 1));
		}

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(list2)
				.bind(slot("result")).toInstance(result2)
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		DataflowGraph graph = Injector.of(common).getInstance(DataflowGraph.class);

		SortedDataset<Long, TestItem> items = repartition_Sort(sortedDatasetOfList("items",
				TestItem.class, Long.class, new TestKeyFunction(), new TestComparator()));

		DatasetListConsumer<?> consumerNode = listConsumer(items, "result");

		consumerNode.compileInto(graph);

		System.out.println(graph.toGraphViz(true));

		await(graph.execute().whenComplete(() -> {
			server1.close();
			server2.close();
		}));

		assertEquals(result1.getList(), list1);
		assertEquals(result2.getList(), list2);
	}

	static InetSocketAddress getFreeListenAddress() throws UnknownHostException {
		return new InetSocketAddress(InetAddress.getByName("127.0.0.1"), getFreePort());
	}
}
