package io.global.globalfs;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsCheckpoint;
import io.global.globalfs.api.GlobalFsFileSystem;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.client.GlobalFsAdapter;
import io.global.globalfs.server.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class GlobalFsTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Eventloop eventloop;
	private ExecutorService executor;
	private GlobalFsAdapter adapter;

	private final KeyPair firstKeys = KeyPair.generate();
	private final KeyPair secondKeys = KeyPair.generate();

	@Before
	public void setUp() throws IOException {
		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		executor = Executors.newSingleThreadExecutor();

		FsClient storage = LocalFsClient.create(eventloop, executor, temporaryFolder.newFolder().toPath());
		DiscoveryService discoveryService = new LocalDiscoveryService();

		RawNodeFactory clientFactory = new RawNodeFactory() {
			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return new GlobalFsLocalNode(serverId, discoveryService, this,
						(group, fsName) ->
								new RemoteFsFileSystem(group, fsName, storage.subfolder(fsName), new RuntimeCheckpointStorage()),
						() -> Duration.ofMinutes(5));
			}
		};
		FileSystemFactory fileSystemFactory = (group, fsName) ->
				new RemoteFsFileSystem(group, fsName, storage, new RuntimeCheckpointStorage());

		//noinspection ConstantConditions - all these nulls
		GlobalFsNode client = new GlobalFsLocalNode(null, discoveryService, clientFactory, fileSystemFactory, () -> Duration.ofMinutes(5));

		GlobalFsFileSystem fs = client.getFileSystem(GlobalFsName.of(firstKeys, "testFs")).getResult();

		adapter = new GlobalFsAdapter(fs, firstKeys, pp -> pp + ThreadLocalRandom.current().nextInt(5, 50));
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void test() {
		SerialSupplier.of(
				ByteBuf.wrapForReading("hello, this is a test buffer data #01\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #02\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #03\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #04\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #05\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #06\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #07\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #08\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #09\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #10\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #11\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #12\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #13\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #14\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #15\n".getBytes(UTF_8))
		).streamTo(adapter.uploadSerial("test1.txt", 0))
				.thenCompose($ -> adapter.downloadSerial("test1.txt", 10, 380 - 10 - 19).toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals("s is a test buffer data #01\n" +
										"hello, this is a test buffer data #02\n" +
										"hello, this is a test buffer data #03\n" +
										"hello, this is a test buffer data #04\n" +
										"hello, this is a test buffer data #05\n" +
										"hello, this is a test buffer data #06\n" +
										"hello, this is a test buffer data #07\n" +
										"hello, this is a test buffer data #08\n" +
										"hello, this is a test buffer data #09\n" +
										"hello, this is a te",
								buf.asString(UTF_8))))
				.thenCompose($ -> adapter.downloadSerial("test1.txt", 64, 259).toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals("er data #02\n" +
										"hello, this is a test buffer data #03\n" +
										"hello, this is a test buffer data #04\n" +
										"hello, this is a test buffer data #05\n" +
										"hello, this is a test buffer data #06\n" +
										"hello, this is a test buffer data #07\n" +
										"hello, this is a test buffer data #08\n" +
										"hello, this is a te",
								buf.asString(UTF_8))))
				.thenCompose($ -> adapter.downloadSerial("test1.txt", 228, 37).toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals("hello, this is a test buffer data #07", buf.asString(UTF_8))))
				.thenCompose($ -> adapter.downloadSerial("test1.txt").toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals(570, buf.asString(UTF_8).length())));

		eventloop.run();
	}

	private static final class RuntimeCheckpointStorage implements CheckpointStorage {
		private Map<String, Map<Long, SignedData<GlobalFsCheckpoint>>> storage = new HashMap<>();

		@Override
		public Stage<long[]> getCheckpoints(String filename) {
			Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
			if (checkpoints == null) {
				return Stage.of(new long[0]);
			}
			return Stage.of(checkpoints.keySet().stream().mapToLong(Long::longValue).sorted().toArray());
		}

		@Override
		public Stage<SignedData<GlobalFsCheckpoint>> loadCheckpoint(String filename, long position) {
			Map<Long, SignedData<GlobalFsCheckpoint>> checkpoints = storage.get(filename);
			if (checkpoints == null) {
				return Stage.of(null);
			}
			return Stage.of(checkpoints.get(position));
		}

		@Override
		public Stage<Void> saveCheckpoint(String filename, SignedData<GlobalFsCheckpoint> checkpoint) {
			storage.computeIfAbsent(filename, $ -> new HashMap<>()).put(checkpoint.getData().getPosition(), checkpoint);
			return Stage.of(null);
		}
	}
}
