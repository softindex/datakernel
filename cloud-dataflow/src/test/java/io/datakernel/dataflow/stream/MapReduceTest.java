package io.datakernel.dataflow.stream;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.datakernel.dataflow.server.Collector;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.dataflow.server.DataflowServer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamReducers.ReducerToAccumulator;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.dataflow.di.DatasetIdImpl.datasetId;
import static io.datakernel.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.datakernel.dataflow.stream.DataflowTest.createCommon;
import static io.datakernel.dataflow.stream.DataflowTest.getFreeListenAddress;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MapReduceTest {

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

		Module common = createCommon(executor, temporaryFolder.newFolder().toPath(), asList(new Partition(address1), new Partition(address2)))
				.bind(new Key<StructuredCodec<StringKeyFunction>>() {}).toInstance(ofObject(StringKeyFunction::new))
				.bind(new Key<StructuredCodec<StringComparator>>() {}).toInstance(ofObject(StringComparator::new))
				.bind(new Key<StructuredCodec<StringMapFunction>>() {}).toInstance(ofObject(StringMapFunction::new))
				.bind(new Key<StructuredCodec<StringReducer>>() {}).toInstance(ofObject(StringReducer::new))
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.build();

		Module serverModule1 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						"dog",
						"cat",
						"horse",
						"cat"))
				.build();

		Module serverModule2 = ModuleBuilder.create()
				.install(common)
				.bind(datasetId("items")).toInstance(asList(
						"dog",
						"cat"))
				.build();

		DataflowServer server1 = Injector.of(serverModule1).getInstance(DataflowServer.class).withListenAddress(address1);
		DataflowServer server2 = Injector.of(serverModule2).getInstance(DataflowServer.class).withListenAddress(address2);

		server1.listen();
		server2.listen();

		Injector clientInjector = Injector.of(common);
		DataflowClient client = clientInjector.getInstance(DataflowClient.class);
		DataflowGraph graph = clientInjector.getInstance(DataflowGraph.class);

		Dataset<String> items = datasetOfList("items", String.class);
		Dataset<StringCount> mappedItems = map(items, new StringMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = sort_Reduce_Repartition_Reduce(mappedItems,
				new StringReducer(), String.class, new StringKeyFunction(), new StringComparator());
		Collector<StringCount> collector = new Collector<>(reducedItems, client);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();

		resultSupplier.streamTo(resultConsumer).whenComplete(assertComplete());

		await(graph.execute()
				.whenComplete(assertComplete($ -> {
					server1.close();
					server2.close();
				})));

		System.out.println(resultConsumer.getList());

		assertEquals(new HashSet<>(asList(
				new StringCount("cat", 3),
				new StringCount("dog", 2),
				new StringCount("horse", 1))), new HashSet<>(resultConsumer.getList()));
	}

	public static class StringReducer extends ReducerToAccumulator<String, StringCount, StringCount> {
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

	public static class StringMapFunction implements Function<String, StringCount> {
		@Override
		public StringCount apply(String s) {
			return new StringCount(s, 1);
		}
	}

	public static class StringKeyFunction implements Function<StringCount, String> {
		@Override
		public String apply(StringCount stringCount) {
			return stringCount.s;
		}
	}

	public static class StringComparator implements Comparator<String> {
		@Override
		public int compare(String s1, String s2) {
			return s1.compareTo(s2);
		}
	}
}
