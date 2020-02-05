package io.datakernel.dataflow.stream;

import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.datakernel.dataflow.server.*;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamReducers.ReducerToAccumulator;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
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
		DataflowSerialization serialization = DataflowSerialization.create()
				.withCodec(TestKeyFunction.class, ofObject(TestKeyFunction::new))
				.withCodec(TestMapFunction.class, ofObject(TestMapFunction::new))
				.withCodec(TestComparator.class, ofObject(TestComparator::new))
				.withCodec(TestReducer.class, ofObject(TestReducer::new));

		DataflowClient client = new DataflowClient(serialization);

		DataflowEnvironment environment = DataflowEnvironment.create()
				.setInstance(DataflowSerialization.class, serialization)
				.setInstance(DataflowClient.class, client)
				.setInstance(StreamSorterStorageFactory.class, FACTORY_STUB);

		DataflowEnvironment environment1 = environment.extend()
				.with("items", asList(
						"dog",
						"cat",
						"horse",
						"cat"));
		DataflowEnvironment environment2 = environment.extend()
				.with("items", asList(
						"dog",
						"cat"));

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();
		server1 = new DataflowServer(Eventloop.getCurrentEventloop(), environment1).withListenAddress(address1);
		server2 = new DataflowServer(Eventloop.getCurrentEventloop(), environment2).withListenAddress(address2);
		server1.listen();
		server2.listen();

		DataflowGraph graph = new DataflowGraph(client, serialization, asList(new Partition(address1), new Partition(address2)));

		Dataset<String> items = datasetOfList("items", String.class);
		Dataset<StringCount> mappedItems = map(items, new TestMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = sort_Reduce_Repartition_Reduce(mappedItems,
				new TestReducer(), String.class, new TestKeyFunction(), new TestComparator());
		Collector<StringCount> collector = new Collector<>(reducedItems, StringCount.class, client);
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
