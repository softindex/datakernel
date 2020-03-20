package io.datakernel.launchers.dataflow;

import io.datakernel.config.Config;
import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.dataset.impl.DatasetListConsumer;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.datakernel.dataflow.server.Collector;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.dataflow.server.DataflowEnvironment;
import io.datakernel.dataflow.server.DataflowSerialization;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamReducers.ReducerToAccumulator;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.launchers.dataflow.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@Ignore("manual")
public class DataflowServerTest {

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

	public static class StringFunction implements Function<String, String> {
		@Override
		public String apply(String string) {
			return string;
		}
	}

	public static class DatagraphSerializationModule extends AbstractModule {
		@Provides
		private DataflowSerialization serialization() {
			return DataflowSerialization.create()
					.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new))
					.withCodec(TestMapFunction.class, ofObject(TestMapFunction::new))
					.withCodec(TestComparator.class, ofObject(TestComparator::new))
					.withCodec(TestReducer.class, ofObject(TestReducer::new))
					.withCodec(StringFunction.class, ofObject(StringFunction::new));
		}
	}

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final int PORT_1 = 9001;
	private static final int PORT_2 = 9002;

	@Test
	public void startServer1() throws Exception {
		startServer(PORT_1, asList("dog", "cat", "horse", "cat"));
	}

	@Test
	public void startServer2() throws Exception {
		startServer(PORT_2, asList("dog", "cat"));
	}

	private void startServer(int port, List<String> words) throws Exception {
		new DataflowServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return new DatagraphSerializationModule();
			}

			@Override
			protected Module getOverrideModule() {
				return Module.create().bind(Config.class).toInstance(Config.create().with("dataflow.server.listenAddresses", String.valueOf(port)));
			}

			@Provides
			public DataflowEnvironment environment(DataflowSerialization serialization) {
				return DataflowEnvironment.create()
						.setInstance(DataflowSerialization.class, serialization)
						.setInstance(DataflowClient.class, new DataflowClient(serialization))
						.setInstance(StreamSorterStorageFactory.class, FACTORY_STUB)
						.with("items", words);
			}
		}.launch(new String[0]);
	}

	@Test
	public void testMapReduce() throws Exception {
		Injector injector = Injector.of(new DatagraphSerializationModule());
		DataflowSerialization serialization = injector.getInstance(DataflowSerialization.class);
		DataflowClient client = new DataflowClient(serialization);
		Partition partition1 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_1));
		Partition partition2 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_2));
		DataflowGraph graph = new DataflowGraph(client, serialization, asList(partition1, partition2));

		Dataset<String> items = datasetOfList("items", String.class);
		Dataset<StringCount> mappedItems = map(items, new TestMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = sort_Reduce_Repartition_Reduce(mappedItems,
				new TestReducer(), String.class, new TestKeyFunction(), new TestComparator());
		Collector<StringCount> collector = new Collector<>(reducedItems, StringCount.class, client);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create();
		resultSupplier.streamTo(resultConsumer);

		System.out.println(graph);
		await(graph.execute().whenException(resultConsumer::closeEx));

		assertEquals(asList(new StringCount("cat", 3), new StringCount("dog", 2), new StringCount("horse", 1)), resultConsumer.getList());
	}

	@Test
	public void testRepartitionAndSort() throws Exception {
		Injector injector = Injector.of(new DatagraphSerializationModule());
		DataflowSerialization serialization = injector.getInstance(DataflowSerialization.class);
		DataflowClient client = new DataflowClient(serialization);
		Partition partition1 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_1));
		Partition partition2 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_2));
		DataflowGraph graph = new DataflowGraph(client, serialization, asList(partition1, partition2));

		Dataset<String> items = datasetOfList("items", String.class);
		Dataset<String> sorted = repartition_Sort(localSort(items, String.class, new StringFunction(), new TestComparator()));
		DatasetListConsumer<?> consumerNode = listConsumer(sorted, "result");
		consumerNode.compileInto(graph);

		System.out.println(graph);
		await(graph.execute());
	}
}
