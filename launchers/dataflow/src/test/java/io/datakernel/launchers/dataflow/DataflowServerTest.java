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
import io.datakernel.datastream.processor.Sharder;
import io.datakernel.datastream.processor.Sharders;
import io.datakernel.datastream.processor.StreamReducers.ReducerToAccumulator;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.EventloopRule;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.launchers.dataflow.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DataflowServerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final int PORT_1 = getFreePort();
	private static final int PORT_2 = getFreePort();

	private TestServerLauncher serverLauncher1;
	private TestServerLauncher serverLauncher2;

	@After
	public void tearDown() {
		serverLauncher1.shutdown();
		serverLauncher2.shutdown();
	}

	@Test
	public void testMapReduceSimple() throws Exception {
		launchServers(asList("dog", "cat", "horse", "cat"), asList("dog", "cat"), false);
		List<StringCount> result = new ArrayList<>();
		await(mapReduce(result));
		assertEquals(asList(new StringCount("cat", 3), new StringCount("dog", 2), new StringCount("horse", 1)), result);
	}

	@Test
	public void testMapReduceOneMillionStrings() throws Exception {
		List<String> words1 = createOneMillionStrings();
		List<String> words2 = createOneMillionStrings();
		launchServers(words1, words2, false);
		List<StringCount> result = new ArrayList<>();
		await(mapReduce(result));

		// manual map reduce for assertion
		Sharder<String> sharder = Sharders.byHash(2);
		List<StringCount> expected = Stream.concat(words1.stream(), words2.stream())
				.collect(groupingBy(Function.identity()))
				.values().stream()
				.map(strings -> new StringCount(strings.get(0), strings.size()))
				.sorted(Comparator.<StringCount, Integer>comparing(stringCount -> sharder.shard(stringCount.s)).thenComparing(stringCount -> stringCount.s))
				.collect(toList());

		assertEquals(expected, result);
	}

	@Test
	public void testMapReduceWithMalformedServer() throws Exception {
		launchServers(asList("dog", "cat", "horse", "cat"), asList("dog", "cat"), true);
		Throwable exception = awaitException(mapReduce(new ArrayList<>()));

		assertThat(exception.getMessage(), containsString("Error on remote server"));
	}

	@Test
	public void testRepartitionAndSortSimple() throws Exception {
		List<String> result1 = new ArrayList<>();
		List<String> result2 = new ArrayList<>();
		launchServers(asList("dog", "cat", "horse", "cat", "cow"), asList("dog", "cat", "cow"), result1, result2, false);
		await(repartitionAndSort());

		assertEquals(asList("cat", "cat", "cat", "dog", "dog"), result1);
		assertEquals(asList("cow", "cow", "horse"), result2);
	}

	@Test
	public void testRepartitionAndSortOneMillionStrings() throws Exception {
		List<String> result1 = new ArrayList<>();
		List<String> result2 = new ArrayList<>();
		List<String> words1 = createOneMillionStrings();
		List<String> words2 = createOneMillionStrings();
		launchServers(words1, words2, result1, result2, false);
		await(repartitionAndSort());

		// manual repartition and sort for assertion
		Sharder<String> sharder = Sharders.byHash(2);
		Map<Integer, List<String>> expected = Stream.concat(words1.stream(), words2.stream())
				.sorted()
				.collect(groupingBy(sharder::shard, LinkedHashMap::new, toList()));

		assertEquals(expected.get(0), result1);
		assertEquals(expected.get(1), result2);
	}

	@Test
	public void testRepartitionAndSortWithMalformedServer() throws Exception {
		launchServers(asList("dog", "cat", "horse", "cat", "cow"), asList("dog", "cat", "cow"), true);
		Throwable exception = awaitException(repartitionAndSort());

		assertThat(exception.getMessage(), containsString("Error on remote server"));
	}

	// region stubs & helpers
	private Promise<Void> mapReduce(List<StringCount> result) throws UnknownHostException {
		Injector injector = Injector.of(new DatagraphSerializationModule());
		DataflowSerialization serialization = injector.getInstance(DataflowSerialization.class);
		DataflowClient client = new DataflowClient(serialization);
		Partition partition1 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_1));
		Partition partition2 = new Partition(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT_2));
		DataflowGraph graph = new DataflowGraph(client, serialization, asList(partition1, partition2));

		Dataset<String> items = datasetOfList("items", String.class);
		Dataset<StringCount> mappedItems = map(items, new TestMapFunction(), StringCount.class);
		Dataset<StringCount> reducedItems = splitSortReduce_Repartition_Reduce(mappedItems,
				new TestReducer(), new TestKeyFunction(), new TestComparator());
		Collector<StringCount> collector = new Collector<>(reducedItems, client);
		StreamSupplier<StringCount> resultSupplier = collector.compile(graph);
		StreamConsumerToList<StringCount> resultConsumer = StreamConsumerToList.create(result);
		resultSupplier.streamTo(resultConsumer);

		return graph.execute().whenException(resultConsumer::closeEx);
	}

	private Promise<Void> repartitionAndSort() throws UnknownHostException {
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
		return graph.execute();
	}

	private static List<String> createOneMillionStrings() {
		List<String> list = new ArrayList<>(1_000_000);
		Random random = new Random();
		for (int i = 0; i < 1_000_000; i++) {
			list.add(Integer.toString(random.nextInt(100_000)));
		}
		return list;
	}

	private void launchServers(List<String> server1Words, List<String> server2Words, boolean oneMalformed) {
		launchServers(server1Words, server2Words, new ArrayList<>(), new ArrayList<>(), oneMalformed);
	}

	private void launchServers(List<String> server1Words, List<String> server2Words, List<String> server1Result, List<String> server2Result, boolean oneMalformed) {
		serverLauncher1 = launchServer(PORT_1, server1Words, server1Result, oneMalformed);
		serverLauncher2 = launchServer(PORT_2, server2Words, server2Result, false);
	}

	private TestServerLauncher launchServer(int port, List<String> words, List<String> result, boolean malformed) {
		CountDownLatch latch = new CountDownLatch(1);
		TestServerLauncher launcher = new TestServerLauncher(port, words, result, malformed, latch);
		new Thread(() -> {
			try {
				launcher.launch(new String[0]);
			} catch (Exception e) {
				throw new AssertionError(e);
			}
		}).start();
		try {
			latch.await();
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		}
		return launcher;
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

	private static final class TestServerLauncher extends DataflowServerLauncher {
		private final int port;
		private final List<String> words;
		private final List<String> result;
		private final boolean malformed;
		private final CountDownLatch latch;

		private TestServerLauncher(int port, List<String> words, List<String> result, boolean malformed, CountDownLatch latch) {
			this.port = port;
			this.words = words;
			this.result = result;
			this.malformed = malformed;
			this.latch = latch;
		}

		@Override
		protected Module getBusinessLogicModule() {
			return new DatagraphSerializationModule();
		}

		@Override
		protected Module getOverrideModule() {
			return Module.create()
					.bind(Config.class).toInstance(Config.create().with("dataflow.server.listenAddresses", String.valueOf(port)))
					.bind(Eventloop.class).toInstance(Eventloop.create().withCurrentThread());
		}

		@Provides
		public DataflowEnvironment environment(DataflowSerialization serialization) {
			return DataflowEnvironment.create()
					.setInstance(DataflowSerialization.class, serialization)
					.setInstance(DataflowClient.class, new DataflowClient(serialization))
					.setInstance(StreamSorterStorageFactory.class, FACTORY_STUB)
					.with(malformed ? "" : "items", words)
					.with("result", StreamConsumerToList.create(result));
		}

		@Override
		protected void run() throws Exception {
			latch.countDown();
			awaitShutdown();
		}
	}
	// endregion
}
