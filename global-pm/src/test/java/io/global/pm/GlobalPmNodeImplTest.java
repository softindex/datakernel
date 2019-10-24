package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.pm.util.BinaryDataFormats.RAW_MESSAGE_CODEC;
import static io.global.pm.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class GlobalPmNodeImplTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final InMemoryAnnouncementStorage ANNOUNCEMENT_STORAGE = new InMemoryAnnouncementStorage();
	private static final InMemorySharedKeyStorage SHARED_KEY_STORAGE = new InMemorySharedKeyStorage();
	private static final KeyPair KEYS = KeyPair.generate();

	private MessageStorage intermediateStorage;
	private GlobalPmNodeImpl intermediate;
	private MessageStorage master1Storage;
	private GlobalPmNodeImpl master1;
	private MessageStorage master2Storage;
	private GlobalPmNodeImpl master2;
	private FailingPmNode failing;
	private DiscoveryService discoveryService;
	private CurrentTimeProvider now;

	@Before
	public void setUp() {
		ANNOUNCEMENT_STORAGE.clear();
		SHARED_KEY_STORAGE.clear();
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		discoveryService = LocalDiscoveryService.create(eventloop, ANNOUNCEMENT_STORAGE, SHARED_KEY_STORAGE);
		intermediateStorage = new MapMessageStorage();
		master1Storage = new MapMessageStorage();
		master2Storage = new MapMessageStorage();
		now = CurrentTimeProvider.ofTimeSequence(10, 10);
		intermediate = createNode("intermediate", intermediateStorage);
		master1 = createNode("master1", master1Storage);
		master2 = createNode("master2", master2Storage);
		announceMasters();
	}

	@Test
	public void testSend() {
		SignedData<RawMessage> message = createMessage(1, false);
		await(intermediate.send(KEYS.getPubKey(), "test", message));
		assertStorages(message, storage -> storage.poll(KEYS.getPubKey(), "test"));
	}

	@Test
	public void testSendTombstone() {
		SignedData<RawMessage> message = createMessage(1, false);
		await(intermediate.send(KEYS.getPubKey(), "test", message));
		assertStorages(message, node -> node.poll(KEYS.getPubKey(), "test"));
		SignedData<RawMessage> tombstone = createMessage(1, true);
		await(intermediate.send(KEYS.getPubKey(), "test", tombstone));
		assertStorages(null, storage -> storage.poll(KEYS.getPubKey(), "test"));
	}

	@Test
	public void testUpload() {
		List<SignedData<RawMessage>> messages = Arrays.asList(
				createMessage(1, false),
				createMessage(2, false),
				createMessage(3, false),
				createMessage(4, false),
				createMessage(5, false)
		);
		await(ChannelSupplier.ofIterable(messages).streamTo(intermediate.upload(KEYS.getPubKey(), "test")));
		assertStorages(messages, storage -> ChannelSupplier.ofPromise(storage.download(KEYS.getPubKey(), "test")).toList());
	}

	@Test
	public void testDownloadByTimestamp() {
		List<SignedData<RawMessage>> messages = Arrays.asList(
				createMessage(1, 100, false),
				createMessage(2, 200, false),
				createMessage(3, 300, false),
				createMessage(4, 400, false),
				createMessage(5, 500, false)
		);
		await(ChannelSupplier.ofIterable(messages).streamTo(intermediate.upload(KEYS.getPubKey(), "test")));
		assertNodes(messages.subList(1, 5), node -> ChannelSupplier.ofPromise(node.download(KEYS.getPubKey(), "test", 100)).toList());
		assertNodes(messages.subList(2, 5), node -> ChannelSupplier.ofPromise(node.download(KEYS.getPubKey(), "test", 200)).toList());
		assertNodes(messages.subList(3, 5), node -> ChannelSupplier.ofPromise(node.download(KEYS.getPubKey(), "test", 300)).toList());
		assertNodes(messages.subList(4, 5), node -> ChannelSupplier.ofPromise(node.download(KEYS.getPubKey(), "test", 400)).toList());
		assertNodes(emptyList(), node -> ChannelSupplier.ofPromise(node.download(KEYS.getPubKey(), "test", 500)).toList());
	}

	@Test
	public void testFetch() {
		SignedData<RawMessage> message = createMessage(1, false);
		await(master1Storage.put(KEYS.getPubKey(), "test", message));
		assertEquals(message, await(master1.poll(KEYS.getPubKey(), "test")));
		assertNull(await(master2.poll(KEYS.getPubKey(), "test")));
		await(master2.fetch());
		assertEquals(message, await(master2.poll(KEYS.getPubKey(), "test")));
	}

	@Test
	public void testPush() {
		SignedData<RawMessage> message = createMessage(1, false);
		await(master1Storage.put(KEYS.getPubKey(), "test", message));
		assertEquals(message, await(master1.poll(KEYS.getPubKey(), "test")));
		assertNull(await(master2.poll(KEYS.getPubKey(), "test")));
		await(master1.push());
		assertEquals(message, await(master2.poll(KEYS.getPubKey(), "test")));
	}

	@Test
	public void testList() {
		await(ChannelSupplier.of(createMessage(1, false)).streamTo(intermediate.upload(KEYS.getPubKey(), "test1")));
		await(ChannelSupplier.of(createMessage(2, true)).streamTo(intermediate.upload(KEYS.getPubKey(), "test2")));
		assertNodes(set("test1", "test2"), node -> node.list(KEYS.getPubKey()));
		await(ChannelSupplier.of(createMessage(3, false)).streamTo(intermediate.upload(KEYS.getPubKey(), "test3")));
		assertNodes(set("test1", "test2", "test3"), node -> node.list(KEYS.getPubKey()));
	}

	@Test
	public void testWithFailingNode() {
		announceFailing();
		assertEquals(emptySet(), await(intermediate.list(KEYS.getPubKey())));
		SignedData<RawMessage> message = createMessage(1, false);
		await(intermediate.send(KEYS.getPubKey(), "test", message));
		assertEquals(singleton("test"), await(intermediate.list(KEYS.getPubKey())));
		assertEquals(message, await(intermediate.poll(KEYS.getPubKey(), "test")));
		List<SignedData<RawMessage>> messages = Arrays.asList(
				createMessage(2, false),
				createMessage(3, false),
				createMessage(4, false),
				createMessage(5, false)
		);
		await(ChannelSupplier.ofIterable(messages).streamTo(intermediate.upload(KEYS.getPubKey(), "test")));
		List<SignedData<RawMessage>> received = await(ChannelSupplier.ofPromise(intermediate.download(KEYS.getPubKey(), "test")).toList());
		assertEquals(concat(singletonList(message), messages), received);
	}

	private <T> void assertStorages(@Nullable T expected, Function<MessageStorage, Promise<T>> action) {
		assertEquals(expected, await(action.apply(intermediateStorage)));
		assertEquals(expected, await(action.apply(master1Storage)));
		assertEquals(expected, await(action.apply(master2Storage)));
	}

	private <T> void assertNodes(@Nullable T expected, Function<GlobalPmNodeImpl, Promise<T>> action) {
		assertEquals(expected, await(action.apply(intermediate)));
		assertEquals(expected, await(action.apply(master1)));
		assertEquals(expected, await(action.apply(master2)));
	}

	private void announceMasters() {
		Set<RawServerId> rawServerIds = set(master1.getId(), master2.getId());
		doAnnounce(rawServerIds);
	}

	private void announceFailing() {
		failing = new FailingPmNode();
		Set<RawServerId> rawServerIds = singleton(new RawServerId("failing"));
		doAnnounce(rawServerIds);
	}

	private void doAnnounce(Set<RawServerId> rawServerIds) {
		AnnounceData announceData = AnnounceData.of(now.currentTimeMillis(), rawServerIds);
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, KEYS.getPrivKey());
		await(discoveryService.announce(KEYS.getPubKey(), signedData));
	}

	private GlobalPmNode getNode(RawServerId serverId) {
		switch (serverId.getServerIdString()) {
			case "master1":
				return master1;
			case "master2":
				return master2;
			case "failing":
				return failing;
			default:
				throw new AssertionError();
		}
	}

	private GlobalPmNodeImpl createNode(String serverId, MessageStorage messageStorage) {
		return GlobalPmNodeImpl.create(new RawServerId(serverId), discoveryService, this::getNode, messageStorage)
				.withUploadCaching(true)
				.withUploadCall(2)
				.withLatencyMargin(Duration.ZERO)
				.withCurrentTimeProvider(now);
	}

	private SignedData<RawMessage> createMessage(long id, boolean tombstone) {
		return createMessage(id, now.currentTimeMillis(), tombstone);
	}

	private SignedData<RawMessage> createMessage(long id, long timestamp, boolean tombstone) {
		byte[] data = tombstone ? null : String.valueOf(id).getBytes(StandardCharsets.UTF_8);
		RawMessage rawMessage = RawMessage.of(id, timestamp, data);
		return SignedData.sign(RAW_MESSAGE_CODEC, rawMessage, KEYS.getPrivKey());
	}

}
