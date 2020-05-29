package io.datakernel.dataflow.stream;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.csp.binary.ByteBufsCodec;
import io.datakernel.csp.dsl.ChannelConsumerTransformer;
import io.datakernel.csp.dsl.ChannelSupplierTransformer;
import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.di.BinarySerializerModule.BinarySerializerLocator;
import io.datakernel.dataflow.di.CodecsModule.Subtypes;
import io.datakernel.dataflow.di.DataflowModule;
import io.datakernel.dataflow.di.DatasetId;
import io.datakernel.dataflow.graph.DataflowContext;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.helper.PartitionedCollector;
import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.node.PartitionedStreamConsumerFactory;
import io.datakernel.dataflow.node.PartitionedStreamSupplierFactory;
import io.datakernel.dataflow.server.DataflowClient;
import io.datakernel.dataflow.server.DataflowServer;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphResponse;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamMerger;
import io.datakernel.datastream.processor.StreamSplitter;
import io.datakernel.datastream.processor.StreamUnion;
import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.net.AbstractServer;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static io.datakernel.dataflow.dataset.Datasets.*;
import static io.datakernel.datastream.StreamSupplier.ofChannelSupplier;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class PartitionedStreamTest {
	private static final String FILENAME = "data.txt";
	private static final Random RANDOM = ThreadLocalRandom.current();
	private static final Function<String, Integer> KEY_FUNCTION = string -> Integer.valueOf(string.split(":")[1]);

	@ClassRule
	public static EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static ByteBufRule byteBufRule = new ByteBufRule();

	private Eventloop serverEventloop;
	private List<RemoteFsServer> sourceFsServers;
	private List<RemoteFsServer> targetFsServers;
	private List<DataflowServer> dataflowServers;

	@Before
	public void setUp() {
		sourceFsServers = new ArrayList<>();
		targetFsServers = new ArrayList<>();
		dataflowServers = new ArrayList<>();
		serverEventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		serverEventloop.keepAlive(true);
		new Thread(serverEventloop).start();
	}

	@After
	public void tearDown() throws Exception {
		serverEventloop.submit(() -> {
			sourceFsServers.forEach(AbstractServer::close);
			dataflowServers.forEach(AbstractServer::close);
		});
		serverEventloop.keepAlive(false);
		Thread serverEventloopThread = serverEventloop.getEventloopThread();
		if (serverEventloopThread != null) {
			serverEventloopThread.join();
		}
	}

	@Test
	public void testNotSortedEqual() throws IOException {
		launchServers(5, 5, 0, false);
		Map<Partition, List<String>> result = collectToMap(false);

		int serverIdx = 0;
		assertEquals(5, result.size());
		for (List<String> items : result.values()) {
			assertEquals(100, items.size());
			for (String item : items) {
				assertTrue(item.startsWith("Server" + serverIdx));
			}
			serverIdx++;
		}
	}

	@Test
	public void testNotSortedMoreFsServers() throws IOException {
		launchServers(10, 3, 0, false);
		Map<Partition, List<String>> result = collectToMap(false);

		assertEquals(3, result.size());

		List<String> firstPartition = get(result, 0);
		assertEquals(400, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0", "Server3", "Server6", "Server9");

		List<String> secondPartition = get(result, 1);
		assertEquals(300, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1", "Server4", "Server7");

		List<String> thirdPartition = get(result, 2);
		assertEquals(300, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2", "Server5", "Server8");
	}

	@Test
	public void testNotSortedMoreDataflowServers() throws IOException {
		launchServers(3, 10, 0, false);
		Map<Partition, List<String>> result = collectToMap(false);

		assertEquals(10, result.size());

		List<String> firstPartition = get(result, 0);
		assertEquals(100, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0");

		List<String> secondPartition = get(result, 1);
		assertEquals(100, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1");

		List<String> thirdPartition = get(result, 2);
		assertEquals(100, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2");

		for (int i = 3; i < 10; i++) {
			List<String> ithPartition = get(result, i);
			assertEquals(0, ithPartition.size());
		}
	}

	@Test
	public void testSortedEqual() throws IOException {
		launchServers(5, 5, 0, true);
		Map<Partition, List<String>> result = collectToMap(true);

		assertEquals(5, result.size());
		assertSorted(result.values());

		for (int i = 0; i < 5; i++) {
			List<String> ithPartition = get(result, i);
			assertEquals(100, ithPartition.size());
		}
	}

	@Test
	public void testSortedMoreFsServers() throws IOException {
		launchServers(10, 3, 0, true);
		Map<Partition, List<String>> result = collectToMap(true);

		assertEquals(3, result.size());
		assertSorted(result.values());

		List<String> firstPartition = get(result, 0);
		assertEquals(400, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0", "Server3", "Server6", "Server9");

		List<String> secondPartition = get(result, 1);
		assertEquals(300, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1", "Server4", "Server7");

		List<String> thirdPartition = get(result, 2);
		assertEquals(300, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2", "Server5", "Server8");
	}

	@Test
	public void testSortedMoreDataflowServers() throws IOException {
		launchServers(3, 10, 0, true);
		Map<Partition, List<String>> result = collectToMap(true);

		assertEquals(10, result.size());
		assertSorted(result.values());

		List<String> firstPartition = get(result, 0);
		assertEquals(100, firstPartition.size());
		assertItemPrefixes(firstPartition, "Server0");

		List<String> secondPartition = get(result, 1);
		assertEquals(100, secondPartition.size());
		assertItemPrefixes(secondPartition, "Server1");

		List<String> thirdPartition = get(result, 2);
		assertEquals(100, thirdPartition.size());
		assertItemPrefixes(thirdPartition, "Server2");

		for (int i = 3; i < 10; i++) {
			List<String> ithPartition = get(result, i);
			assertEquals(0, ithPartition.size());
		}
	}

	@Test
	public void testPropagationToTargetFs() throws IOException {
		launchServers(10, 5, 2, false);
		filterOddAndPropagateToTarget(false);
	}

	// region modules
	private Module createServerModule() {
		return Modules.combine(
				DataflowModule.create(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return serverEventloop;
					}

					@Provides
					DataflowServer server(Eventloop eventloop, ByteBufsCodec<DatagraphCommand, DatagraphResponse> codec, BinarySerializerLocator locator, Injector injector) {
						return new DataflowServer(eventloop, codec, locator, injector);
					}

					@Provides
					@Named("source")
					List<FsClient> sourceFsClients(Eventloop eventloop) {
						return createClients(eventloop, sourceFsServers);
					}

					@Provides
					@Named("target")
					List<FsClient> targetFsClients(Eventloop eventloop) {
						return createClients(eventloop, targetFsServers);
					}

					@Provides
					@DatasetId("data source")
					PartitionedStreamSupplierFactory<String> data(@Named("source") List<FsClient> fsClients) {
						return (partitionIndex, maxPartitions) -> {
							StreamUnion<String> union = StreamUnion.create();
							for (int i = partitionIndex; i < fsClients.size(); i += maxPartitions) {
								ChannelSupplier.ofPromise(fsClients.get(i).download(FILENAME))
										.transformWith(new CSVDecoder())
										.streamTo(union.newInput());
							}
							return union.getOutput();
						};
					}

					@Provides
					@DatasetId("sorted data source")
					PartitionedStreamSupplierFactory<String> dataSorted(@Named("source") List<FsClient> fsClients) {
						return (partitionIndex, maxPartitions) -> {
							StreamMerger<Integer, String> merger = StreamMerger.create(KEY_FUNCTION, Integer::compareTo, false);

							for (int i = partitionIndex; i < fsClients.size(); i += maxPartitions) {
								ChannelSupplier.ofPromise(fsClients.get(i).download(FILENAME))
										.transformWith(new CSVDecoder())
										.streamTo(merger.newInput());
							}
							return merger.getOutput();
						};
					}

					@Provides
					@DatasetId("data target")
					PartitionedStreamConsumerFactory<String> dataUpload(@Named("target") List<FsClient> fsClients) {
						return (partitionIndex, maxPartitions) -> {
							StreamSplitter<String, String> splitter = StreamSplitter.create((item, acceptors) -> {
								acceptors[0].accept(item);
							});

							for (int i = partitionIndex; i < fsClients.size(); i += maxPartitions) {
								splitter.newOutput()
										.streamTo(ChannelConsumer.ofPromise(fsClients.get(i).upload(FILENAME))
												.transformWith(new CSVEncoder()));
							}


							return splitter.getInput();
						};
					}

					@Provides
					@DatasetId("sorted data target")
					PartitionedStreamConsumerFactory<String> dataUploadSorted(@Named("target") List<FsClient> fsClients) {
						return (partitionIndex, maxPartitions) -> {
							StreamSplitter<String, String> splitter = StreamSplitter.create((item, acceptors) -> {
								acceptors[0].accept(item);

							});
							// splitter.newOutput().streamTo();


							return splitter.getInput();
						};
					}

				}
		);
	}

	private static Module createClientModule() {
		return Modules.combine(
				DataflowModule.create(),
				new AbstractModule() {
					@Provides
					Eventloop eventloop() {
						return Eventloop.getCurrentEventloop();
					}

					@Provides
					DataflowClient client(Executor executor,
							ByteBufsCodec<DatagraphResponse, DatagraphCommand> codec,
							BinarySerializerLocator locator) throws IOException {
						return new DataflowClient(executor, Files.createTempDirectory("").toAbsolutePath(), codec, locator);
					}

					@Provides
					Executor executor() {
						return newSingleThreadExecutor();
					}
				}
		);
	}
	// endregion

	// region helpers
	private static List<FsClient> createClients(Eventloop eventloop, List<RemoteFsServer> servers) {
		return servers.stream()
				.flatMap(remoteFsServer -> remoteFsServer.getListenAddresses().stream())
				.map(inetSocketAddress -> RemoteFsClient.create(eventloop, inetSocketAddress))
				.collect(Collectors.toList());
	}

	private void assertSorted(Collection<List<String>> result) {
		for (List<String> items : result) {
			int lastKey = 0;
			for (String item : items) {
				int key = KEY_FUNCTION.apply(item);
				assertTrue(key >= lastKey);
				lastKey = key;
			}
		}
	}

	private static List<String> get(Map<Partition, List<String>> result, int idx) {
		Iterator<List<String>> iterator = result.values().iterator();
		for (int i = 0; i < idx + 1; i++) {
			List<String> list = iterator.next();
			if (i == idx) {
				return list;
			}
		}
		throw new AssertionError();
	}

	private static void assertItemPrefixes(List<String> items, String... prefixes) {
		Map<String, List<String>> collected = items.stream().collect(groupingBy(item -> item.split(":")[0]));
		assertEquals(set(prefixes), collected.keySet());
		int size = first(collected.values()).size();
		assertTrue(size > 0);
		for (List<String> value : collected.values()) {
			assertEquals(size, value.size());
		}
	}

	private Map<Partition, List<String>> collectToMap(boolean sorted) throws IOException {
		Injector injector = Injector.of(createClientModule());
		StructuredCodec<Node> nodeCodec = injector.getInstance(new Key<StructuredCodec<Node>>() {}.qualified(Subtypes.class));
		DataflowClient client = injector.getInstance(DataflowClient.class);
		DataflowGraph graph = new DataflowGraph(client, toPartitions(dataflowServers), nodeCodec);
		Dataset<String> compoundDataset = datasetOfId(sorted ? "sorted data source" : "data source", String.class);

		PartitionedCollector<String> collector = new PartitionedCollector<>(compoundDataset, client);

		Promise<Map<Partition, List<String>>> resultPromise = collector.compile(graph);
		await(graph.execute());
		return await(resultPromise);
	}

	private void filterOddAndPropagateToTarget(boolean sorted) {
		Injector injector = Injector.of(createClientModule());
		StructuredCodec<Node> nodeCodec = injector.getInstance(new Key<StructuredCodec<Node>>() {}.qualified(Subtypes.class));
		DataflowClient client = injector.getInstance(DataflowClient.class);
		DataflowGraph graph = new DataflowGraph(client, toPartitions(dataflowServers), nodeCodec);
		Dataset<String> compoundDataset = datasetOfId(sorted ? "data_sorted" : "data", String.class);
		Dataset<String> filteredDataset = filter(compoundDataset, string -> KEY_FUNCTION.apply(string) % 2 == 0);
		Dataset<String> consumerDataset = consumerOfId(filteredDataset, sorted ? "sorted data target" : "data target");
		compoundDataset.channels(DataflowContext.of(graph));

		await(graph.execute());
	}

	private void launchServers(int nSourceFsServers, int nDataflowServers, int nTargetFsServers, boolean sorted) throws IOException {
		sourceFsServers.addAll(launchSourceFsServers(nSourceFsServers, sorted));
		targetFsServers.addAll(launchTargetFsServers(nTargetFsServers));
		dataflowServers.addAll(launchDataflowServers(nDataflowServers));
	}

	private List<RemoteFsServer> launchSourceFsServers(int nServers, boolean sorted) throws IOException {
		List<RemoteFsServer> servers = new ArrayList<>();
		for (int i = 0; i < nServers; i++) {
			Path tmp = Files.createTempDirectory("source_server_" + i + "_");
			writeDataFile(tmp, i, sorted);
			RemoteFsServer server = RemoteFsServer.create(serverEventloop, newSingleThreadExecutor(), tmp)
					.withListenPort(getFreePort());
			servers.add(server);
		}
		for (RemoteFsServer server : servers) {
			listen(server);
		}
		return servers;
	}

	private List<RemoteFsServer> launchTargetFsServers(int nServers) throws IOException {
		List<RemoteFsServer> servers = new ArrayList<>();
		for (int i = 0; i < nServers; i++) {
			Path tmp = Files.createTempDirectory("target_server_" + i + "_");
			RemoteFsServer server = RemoteFsServer.create(serverEventloop, newSingleThreadExecutor(), tmp)
					.withListenPort(getFreePort());
			servers.add(server);
		}
		for (RemoteFsServer server : servers) {
			listen(server);
		}
		return servers;
	}


	private static void writeDataFile(Path serverFsPath, int serverIdx, boolean sorted) throws IOException {
		int nItems = 100;
		int nextNumber = RANDOM.nextInt(10);

		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < nItems; i++) {
			if (sorted) {
				nextNumber += RANDOM.nextInt(10);
			} else {
				nextNumber = RANDOM.nextInt(1000);
			}
			stringBuilder.append("Server").append(serverIdx).append(":").append(nextNumber);
			if (i != nItems - 1) {
				stringBuilder.append(',');
			}
		}
		Path path = serverFsPath.resolve(FILENAME);
		Files.write(path, stringBuilder.toString().getBytes(UTF_8));
	}

	private List<DataflowServer> launchDataflowServers(int nPartitions) {
		Module serverModule = createServerModule();
		List<DataflowServer> servers = new ArrayList<>();
		for (int i = 0; i < nPartitions; i++) {
			Injector injector = Injector.of(serverModule);
			DataflowServer server = injector.getInstance(DataflowServer.class);
			server.withListenPort(getFreePort());
			listen(server);
			servers.add(server);
		}
		return servers;
	}

	private void listen(AbstractServer<?> server) {
		serverEventloop.submit(() -> {
			try {
				server.listen();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private static List<Partition> toPartitions(List<DataflowServer> servers) {
		return servers.stream()
				.map(AbstractServer::getListenAddresses)
				.flatMap(Collection::stream)
				.map(Partition::new)
				.collect(toList());
	}
	// endregion

	private static class CSVDecoder implements ChannelSupplierTransformer<ByteBuf, StreamSupplier<String>> {

		@Override
		public StreamSupplier<String> transform(ChannelSupplier<ByteBuf> supplier) {
			BinaryChannelSupplier binaryChannelSupplier = BinaryChannelSupplier.of(supplier);
			return ofChannelSupplier(ChannelSupplier.of(
					() -> binaryChannelSupplier.parse(
							bufs -> {
								for (int i = 0; i < bufs.remainingBytes(); i++) {
									if (bufs.peekByte(i) == ',') {
										ByteBuf buf = bufs.takeExactSize(i);
										bufs.skip(1);
										return buf.asString(UTF_8);
									}
								}
								return null;
							})
							.thenEx((value, e) -> {
								if (e == null) return Promise.of(value);
								if (e == UNEXPECTED_END_OF_STREAM_EXCEPTION) {
									ByteBufQueue bufs = binaryChannelSupplier.getBufs();
									return Promise.of(bufs.isEmpty() ? null : bufs.takeRemaining().asString(UTF_8));
								}
								return Promise.ofException(e);
							}),
					binaryChannelSupplier));
		}
	}

	private static class CSVEncoder implements ChannelConsumerTransformer<ByteBuf, StreamConsumer<String>> {

		@Override
		public StreamConsumer<String> transform(ChannelConsumer<ByteBuf> consumer) {
			throw new UnsupportedOperationException("CSVEncoder#transform is not implemented yet");
		}
	}
}
