package io.global.pm;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.promise.TestUtils.await;
import static io.global.pm.util.BinaryDataFormats.RAW_MESSAGE_CODEC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public final class MessageStorageTest {
	private static final PrivKey SIGNING_SK = KeyPair.generate().getPrivKey();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Parameterized.Parameter()
	public Function<Path, MessageStorage> storageFn;

	private MessageStorage storage;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return asList(
				new Object[]{(Function<Path, MessageStorage>) path -> new MapMessageStorage()
				},
				new Object[]{(Function<Path, MessageStorage>) path -> {
					RocksDbMessageStorage rocksDb = RocksDbMessageStorage.create(
							getCurrentEventloop(),
							newCachedThreadPool(),
							path.resolve("rocksDb").toString());
					await(rocksDb.start());
					return rocksDb;
				}
				}
		);
	}

	@Before
	public void setUp() throws Exception {
		storage = storageFn.apply(temporaryFolder.newFolder().toPath());
	}

	@Test
	public void testUploadMessage() {
		PubKey pubKey = KeyPair.generate().getPubKey();
		SignedData<RawMessage> message = createMessage(1, 100, false);
		await(ChannelSupplier.of(message).streamTo(storage.upload(pubKey, "test")));
		assertEquals(message, await(storage.poll(pubKey, "test")));
	}

	@Test
	public void testPutMessage() {
		PubKey pubKey = KeyPair.generate().getPubKey();
		SignedData<RawMessage> message = createMessage(1, 100, false);
		assertTrue(await(storage.put(pubKey, "test", message)));
		assertEquals(message, await(storage.poll(pubKey, "test")));
	}

	@Test
	public void testPutTombstone() {
		PubKey pubKey = KeyPair.generate().getPubKey();
		SignedData<RawMessage> tombstone = createMessage(1, 100, true);
		assertTrue(await(storage.put(pubKey, "test", tombstone)));
		assertNull(await(storage.poll(pubKey, "test")));
		List<SignedData<RawMessage>> messages = await(ChannelSupplier.ofPromise(storage.download(pubKey, "test")).toList());
		assertEquals(singletonList(tombstone), messages);
	}

	@Test
	public void testUploadTombstone() {
		PubKey pubKey = KeyPair.generate().getPubKey();
		SignedData<RawMessage> tombstone = createMessage(1, 100, true);
		await(ChannelSupplier.of(tombstone).streamTo(storage.upload(pubKey, "test")));
		assertNull(await(storage.poll(pubKey, "test")));
		List<SignedData<RawMessage>> messages = await(ChannelSupplier.ofPromise(storage.download(pubKey, "test")).toList());
		assertEquals(singletonList(tombstone), messages);
	}

	@Test
	public void testOverrideMessageWithTombstone() {
		PubKey pubKey = KeyPair.generate().getPubKey();
		SignedData<RawMessage> message = createMessage(1, 100, false);
		await(storage.put(pubKey, "test", message));
		assertEquals(message, await(storage.poll(pubKey, "test")));
		SignedData<RawMessage> tombstone = createMessage(1, 200, true);
		await(storage.put(pubKey, "test", tombstone));
		assertNull(await(storage.poll(pubKey, "test")));
		List<SignedData<RawMessage>> messages = await(ChannelSupplier.ofPromise(storage.download(pubKey, "test")).toList());
		assertEquals(singletonList(tombstone), messages);
	}

	@Test
	public void testUploadMultipleMessages() {
		PubKey pubKey = KeyPair.generate().getPubKey();
		List<SignedData<RawMessage>> messages = asList(
				createMessage(1, 100, false),
				createMessage(2, 200, false),
				createMessage(3, 300, false)
		);
		await(ChannelSupplier.ofIterable(messages).streamTo(storage.upload(pubKey, "test")));
		List<SignedData<RawMessage>> received = await(ChannelSupplier.ofPromise(storage.download(pubKey, "test")).toList());
		assertEquals(messages, received);
	}

	@Test
	public void testDownloadByTimestamp() {
		PubKey pubKey = KeyPair.generate().getPubKey();
		List<SignedData<RawMessage>> messages = asList(
				createMessage(1, 100, false),
				createMessage(2, 200, false),
				createMessage(3, 300, false),
				createMessage(4, 400, false),
				createMessage(5, 500, false)
		);
		await(ChannelSupplier.ofIterable(messages).streamTo(storage.upload(pubKey, "test")));
		List<SignedData<RawMessage>> received = await(ChannelSupplier.ofPromise(storage.download(pubKey, "test", 300)).toList());
		List<SignedData<RawMessage>> expected = messages.subList(3, messages.size());
		assertEquals(expected, received);
	}

	@Test
	public void testPutToSameSpaceDifferentMailboxes() {
		PubKey pubKey = KeyPair.generate().getPubKey();

		SignedData<RawMessage> message1 = createMessage(1, 100, false);
		SignedData<RawMessage> message2 = createMessage(2, 100, false);
		SignedData<RawMessage> message3 = createMessage(3, 100, false);

		await(ChannelSupplier.of(message1).streamTo(storage.upload(pubKey, "test1")));
		await(ChannelSupplier.of(message2).streamTo(storage.upload(pubKey, "test2")));
		await(ChannelSupplier.of(message3).streamTo(storage.upload(pubKey, "test3")));

		assertEquals(singletonList(message1), await(ChannelSupplier.ofPromise(storage.download(pubKey, "test1")).toList()));
		assertEquals(singletonList(message2), await(ChannelSupplier.ofPromise(storage.download(pubKey, "test2")).toList()));
		assertEquals(singletonList(message3), await(ChannelSupplier.ofPromise(storage.download(pubKey, "test3")).toList()));
	}

	@Test
	public void testUploadToDifferentSpacesSameMailboxes() {
		PubKey pubKey1 = KeyPair.generate().getPubKey();
		PubKey pubKey2 = KeyPair.generate().getPubKey();
		PubKey pubKey3 = KeyPair.generate().getPubKey();

		SignedData<RawMessage> message1 = createMessage(1, 100, false);
		SignedData<RawMessage> message2 = createMessage(2, 100, false);
		SignedData<RawMessage> message3 = createMessage(3, 100, false);

		await(ChannelSupplier.of(message1).streamTo(storage.upload(pubKey1, "test")));
		await(ChannelSupplier.of(message2).streamTo(storage.upload(pubKey2, "test")));
		await(ChannelSupplier.of(message3).streamTo(storage.upload(pubKey3, "test")));

		assertEquals(singletonList(message1), await(ChannelSupplier.ofPromise(storage.download(pubKey1, "test")).toList()));
		assertEquals(singletonList(message2), await(ChannelSupplier.ofPromise(storage.download(pubKey2, "test")).toList()));
		assertEquals(singletonList(message3), await(ChannelSupplier.ofPromise(storage.download(pubKey3, "test")).toList()));
	}

	@Test
	public void testUploadToDifferentSpacesDifferentMailboxes() {
		PubKey pubKey1 = KeyPair.generate().getPubKey();
		PubKey pubKey2 = KeyPair.generate().getPubKey();
		PubKey pubKey3 = KeyPair.generate().getPubKey();

		SignedData<RawMessage> message1 = createMessage(1, 100, false);
		SignedData<RawMessage> message2 = createMessage(2, 100, false);
		SignedData<RawMessage> message3 = createMessage(3, 100, false);

		await(ChannelSupplier.of(message1).streamTo(storage.upload(pubKey1, "test1")));
		await(ChannelSupplier.of(message2).streamTo(storage.upload(pubKey2, "test2")));
		await(ChannelSupplier.of(message3).streamTo(storage.upload(pubKey3, "test3")));

		assertEquals(singletonList(message1), await(ChannelSupplier.ofPromise(storage.download(pubKey1, "test1")).toList()));
		assertEquals(singletonList(message2), await(ChannelSupplier.ofPromise(storage.download(pubKey2, "test2")).toList()));
		assertEquals(singletonList(message3), await(ChannelSupplier.ofPromise(storage.download(pubKey3, "test3")).toList()));
	}

	private static SignedData<RawMessage> createMessage(long id, long timestamp, boolean tombstone) {
		byte[] data = tombstone ? null : String.valueOf(id).getBytes(StandardCharsets.UTF_8);
		RawMessage rawMessage = RawMessage.of(id, timestamp, data);
		return SignedData.sign(RAW_MESSAGE_CODEC, rawMessage, SIGNING_SK);
	}

}
