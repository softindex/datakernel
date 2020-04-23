package io.datakernel.dataflow.stream;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.binary.ByteBufsCodec;
import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.di.BinarySerializersModule;
import io.datakernel.dataflow.di.CodecsModule.Subtypes;
import io.datakernel.dataflow.di.DataflowModule;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.datakernel.dataflow.server.Collector;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.dataflow.server.DataflowServer;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphResponse;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamReducers.ReducerToAccumulator;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.dataflow.di.EnvironmentModule.slot;
import static io.datakernel.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.datakernel.dataflow.stream.DataflowTest.getFreeListenAddress;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MapReduceTest {
	private static DataflowServer server1;
	private static DataflowServer server2;

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

	public static class StringCount {
		@Serialize(order = 0)
		public final String s;
		@Serialize(order = 1)
		public int count;

		public StringCount(@Deserialize("s") String s, @Deserialize("count") int count) {
			this.s = s;
			this.count = count;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof StringCount)) return false;
			StringCount that = (StringCount) o;
			return s.equals(that.s) && count == that.count;
		}

		@Override
		public int hashCode() {
			return Objects.hash(s, count);
		}

		@Override
		public String toString() {
			return "StringCount{s='" + s + '\'' + ", count=" + count + '}';
		}
	}

	@Test
	public void test() throws Exception {

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
				.bind(new Key<StructuredCodec<TestMapFunction>>() {}).toInstance(ofObject(TestMapFunction::new))
				.bind(new Key<StructuredCodec<TestReducer>>() {}).toInstance(ofObject(TestReducer::new))

				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(asList(
						"dog",
						"cat",
						"horse",
						"cat"))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(slot("items")).toInstance(asList(
						"dog",
						"cat"))
				.build();

		server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<String> items = datasetOfList("items", String.class);
		Dataset<StringCount> mappedItems = map(items, new TestMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = sort_Reduce_Repartition_Reduce(mappedItems,
				new TestReducer(), String.class, new TestKeyFunction(), new TestComparator());
		Collector<StringCount> collector = new Collector<>(reducedItems, client);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();
		resultSupplier.streamTo(resultConsumer);

		System.out.println(graph);
		await(cleanUp(graph.execute()));

		assertEquals(asList(new StringCount("cat", 3), new StringCount("dog", 2), new StringCount("horse", 1)), resultConsumer.getList());
	}

	public static class TestReducer extends ReducerToAccumulator<String, StringCount, StringCount> {
		@Override
		public StringCount createAccumulator(String key) {
			return new StringCount(key, 0);
		}

		@Override
		public StringCount accumulate(StringCount accumulator, StringCount value) {
			accumulator.count += value.count;
			return accumulator;
		}

		@Override
		public StringCount combine(StringCount accumulator, StringCount anotherAccumulator) {
			accumulator.count += anotherAccumulator.count;
			return accumulator;
		}
	}

	public static class TestMapFunction implements Function<String, StringCount> {
		@Override
		public StringCount apply(String s) {
			return new StringCount(s, 1);
		}
	}

	public static class TestKeyFunction implements Function<StringCount, String> {
		@Override
		public String apply(StringCount stringCount) {
			return stringCount.s;
		}
	}

	public static class TestComparator implements Comparator<String> {
		@Override
		public int compare(String s1, String s2) {
			return s1.compareTo(s2);
		}
	}

	private static <T> Promise<T> cleanUp(Promise<T> promise) {
		return promise.whenComplete(() -> {
			server1.close();
			server2.close();
		});
	}
}
