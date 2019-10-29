package io.global.pm;

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promises;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.http.RoutingServlet;
import io.datakernel.net.SocketSettings;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggerConfig;
import io.datakernel.test.rules.LoggingRule;
import io.datakernel.util.ref.RefInt;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import io.global.pm.http.GlobalPmNodeServlet;
import io.global.pm.http.HttpGlobalPmNode;
import org.junit.*;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.pm.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.slf4j.event.Level.WARN;

@Ignore("takes too long")
@LoggerConfig(value = WARN)
public final class GlobalPmStressTest {
	private static final KeyPair KEYS = KeyPair.generate();
	private static final String MAIL_BOX = "test";
	private static final int MSG_COUNT = 500_000;
	private static final int MASTER_COUNT = 10;
	private static final int INTERMEDIATE_COUNT = 5;
	private static final Random RANDOM = new Random();
	private static final byte[] MESSAGE = "Hello".getBytes(UTF_8);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final LoggingRule loggingRule = new LoggingRule();

	private final List<NodeWithServer> masters = new ArrayList<>();
	private final List<GlobalPmNodeImpl> intermediates = new ArrayList<>();

	private Set<Long> messageIds;
	private InMemoryAnnouncementStorage announcementStorage;
	private DiscoveryService discoveryService;
	private IAsyncHttpClient client;

	@Before
	public void setUp() {
		System.out.println("Initializing...");
		messageIds = LongStream.range(1, MSG_COUNT + 1).boxed().collect(toSet());

		Eventloop eventloop = Eventloop.getCurrentEventloop();
		announcementStorage = new InMemoryAnnouncementStorage();
		InMemorySharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
		discoveryService = LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
		client = AsyncHttpClient.create(eventloop)
				.withKeepAliveTimeout(Duration.ofSeconds(1))
				.withSocketSettings(SocketSettings.create().withTcpNoDelay(true));

		for (int i = 0; i < INTERMEDIATE_COUNT; i++) {
			GlobalPmNodeImpl intermediate = GlobalPmNodeImpl.create(new RawServerId("intermediate" + i),
					discoveryService, this::getNode, new MapMessageStorage())
					.withUploadCaching(false);
			intermediates.add(intermediate);
		}

		Set<RawServerId> serverIds = new HashSet<>();

		int port = 13153;
		for (int i = 0; i < MASTER_COUNT; i++) {
			RawServerId rawServerId = new RawServerId("http://127.0.0.1:" + ++port);
			serverIds.add(rawServerId);
			NodeWithServer masterNode = new NodeWithServer(rawServerId, port);
			masters.add(masterNode);
		}

		addAnnouncments(serverIds);
		masters.forEach(t -> {
			try {
				t.server.listen();
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		});
	}

	@Test
	public void test() {
		int turnRate = RANDOM.nextInt(500) + 500;

		RefInt counter = new RefInt(0);
		Set<Long> ids = await(
				Promises.loop(messageIds.iterator(),
						AsyncPredicate.of(Iterator::hasNext),
						iterator -> {
							if (++counter.value % 1_000 == 0) {
								System.out.printf("Sending message #%,d\n", counter.value);
								List<Integer> onlineMasters = new ArrayList<>();
								for (int i = 0; i < masters.size(); i++) {
									if (masters.get(i).server.isRunning()) {
										onlineMasters.add(i + 1);
									}
								}
								System.out.println("Currently online master servers are: " + (onlineMasters.isEmpty() ?
										"none" :
										onlineMasters.stream()
												.map(i -> "#" + i)
												.collect(joining(", "))));
							}
							int nodeNumber = RANDOM.nextInt(INTERMEDIATE_COUNT);
							GlobalPmNodeImpl node = intermediates.get(nodeNumber);
							Long id = iterator.next();
							RawMessage message = RawMessage.of(id, System.currentTimeMillis(), MESSAGE);
							SignedData<RawMessage> signedData = SignedData.sign(REGISTRY.get(RawMessage.class), message, KEYS.getPrivKey());

							return node.send(KEYS.getPubKey(), MAIL_BOX, signedData)
									.whenResult($ -> {
										if (id % turnRate == 0) {
											turnOffOrOn();
										}
									})
									.map($ -> iterator);
						})
						.whenResult($ -> masters.forEach(NodeWithServer::turnOn))
						.whenResult($ -> {
							System.out.println("\nBefore syncing masters: ");
							printMasterStats();
							printIntermediateStats();
							System.out.println("\nPushing messages from intermediate nodes...");
						})
						.then($ -> Promises.all(intermediates.stream().map(GlobalPmNodeImpl::push)))
						.whenResult($ -> {
							System.out.println("\nAfter pushing messages from intermediate nodes: ");
							printMasterStats();
							System.out.println("\nSyncing master nodes...");
						})
						.then($ -> Promises.all(masters.stream()
								.map(nodeWithServer -> nodeWithServer.node.fetch())))
						.whenResult($ -> {
							System.out.println("\nAfter syncing masters:");
							printMasterStats();
						})
						.then($ -> ChannelSupplier.ofPromise(intermediates.get(0).download(KEYS.getPubKey(), MAIL_BOX))
								.map(signedData -> signedData.getValue().getId()).toCollector(toSet()))
						.whenComplete(() -> masters.forEach(t -> t.server.close())));
		assertEquals(messageIds, ids);
	}

	private void printMasterStats() {
		printStats(masters.stream().map(nodeWithServer -> nodeWithServer.node), "Master");
	}

	private void printIntermediateStats() {
		printStats(intermediates.stream(), "Intermediate");
	}

	private void printStats(Stream<GlobalPmNodeImpl> nodeStream, String type) {
		Map<RawServerId, Integer> msgCount = nodeStream
				.collect(Collectors.toMap(AbstractGlobalNode::getId,
						master -> ((MapMessageStorage) master.getStorage()).storage
								.getOrDefault(KEYS.getPubKey(), emptyMap())
								.getOrDefault(MAIL_BOX, emptyMap())
								.size()));

		System.out.println(type + " count:");
		msgCount.entrySet().stream()
				.sorted(comparing(e -> e.getKey().getServerIdString()))
				.forEach(e -> System.out.println(e.getKey() + " " + e.getValue() + " messages"));

		System.out.println("Total: " + msgCount.values().stream().mapToInt(value -> value).sum());
	}

	private void turnOffOrOn() {
		masters.forEach(node -> {
			if (RANDOM.nextInt(MASTER_COUNT / 2) == 0) {
				node.turnOn();
			} else {
				node.turnOff();
			}
		});
	}

	private void addAnnouncments(Set<RawServerId> serverIds) {
		AnnounceData announceData = AnnounceData.of(System.currentTimeMillis(), serverIds);
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, KEYS.getPrivKey());
		announcementStorage.addAnnouncements(map(KEYS.getPubKey(), signedData));
	}

	private GlobalPmNode getNode(RawServerId serverId) {
		return HttpGlobalPmNode.create(serverId.getServerIdString(), client);
	}

	private final class NodeWithServer {
		final int port;
		final GlobalPmNodeImpl node;
		AsyncHttpServer server;

		NodeWithServer(RawServerId id, int port) {
			this.node = GlobalPmNodeImpl.create(id, discoveryService, GlobalPmStressTest.this::getNode, new MapMessageStorage())
					.withManagedPublicKey(KEYS.getPubKey());
			this.port = port;
			this.server = createServer();
		}

		void turnOff() {
			this.server.close();
		}

		void turnOn() {
			if (server.getCloseNotification().isComplete()) {
				server = createServer();
			}
			try {
				server.listen();
			} catch (IOException e) {
				throw new AssertionError();
			}
		}

		AsyncHttpServer createServer() {
			return AsyncHttpServer.create(Eventloop.getCurrentEventloop(), RoutingServlet.create()
					.map("/pm/*", GlobalPmNodeServlet.create(node)))
					.withSocketSettings(SocketSettings.create().withTcpNoDelay(true))
					.withListenPort(port)
					.withReadWriteTimeout(Duration.ofDays(1000));
		}
	}
}
