/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.ot.server;

import ch.qos.logback.classic.Level;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.async.RetryPolicy;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import io.datakernel.test.rules.LoggingRule.LoggerConfig;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.time.SteppingCurrentTimeProvider;
import io.datakernel.util.Tuple2;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.EncryptedData;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.api.*;
import io.global.ot.stub.CommitStorageStub;
import io.global.ot.util.FailingDiscoveryService;
import io.global.ot.util.FailingGlobalOTNode;
import io.global.ot.util.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.test.TestUtils.enableLogging;
import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.CollectorsEx.toAll;
import static io.datakernel.util.CollectorsEx.toAny;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static io.global.ot.util.TestUtils.getCommitId;
import static io.global.ot.util.TestUtils.getCommitIds;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class GlobalOTNodeImplTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final LoggingRule loggingRule = new LoggingRule();

	private static final InMemoryAnnouncementStorage ANNOUNCEMENT_STORAGE = new InMemoryAnnouncementStorage();
	private static final InMemorySharedKeyStorage SHARED_KEY_STORAGE = new InMemorySharedKeyStorage();
	private static final KeyPair KEYS = KeyPair.generate();
	private static final PubKey PUB_KEY = KEYS.getPubKey();
	private static final PrivKey PRIV_KEY = KEYS.getPrivKey();
	private static final SimKey SIM_KEY = SimKey.generate();
	private static final Hash HASH = Hash.sha1(SIM_KEY.getAesKey().getKey());
	private static final String REPOSITORY_NAME = "Test repository";
	private static final RepoID REPO_ID = RepoID.of(PUB_KEY, REPOSITORY_NAME);
	private static final byte[] DATA = {1, 2, 3, 4, 5, 6};
	private static final Random RANDOM = new Random();
	private static final CurrentTimeProvider now = SteppingCurrentTimeProvider.create(10, 10);

	private final Map<Integer, Tuple2<CommitStorage, GlobalOTNode>> masters = new HashMap<>();
	private final Map<String, GlobalOTNode> turnedOffNodes = new HashMap<>();

	private DiscoveryService discoveryService;
	private CommitStorage intermediateStorage;
	private GlobalOTNode intermediateNode;
	private static int COMMIT_ID;

	@Parameter()
	public Function<Path, CommitStorage> storageFn;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{(Function<Path, CommitStorage>) path -> new CommitStorageStub()},
				new Object[]{(Function<Path, CommitStorage>) path -> {
					CommitStorageRocksDb rocksDb = CommitStorageRocksDb.create(
							getCurrentEventloop(),
							path.resolve("rocksDb").toString());
					await(rocksDb.start());
					return rocksDb;
				}
				}
		);
	}

	private void initializeMasters(Integer numberOfMasters) {
		initializeMasters(numberOfMasters, now);
	}

	private void initializeMasters(Integer numberOfMasters, CurrentTimeProvider timeProvider) {
		clearDiscovery();
		IntStream.rangeClosed(1, numberOfMasters).boxed().forEach(id -> {
			try {
				String folder = "master" + id;
				CommitStorage storage;
				Path resolved = temporaryFolder.getRoot().toPath().resolve(folder);
				if (Files.exists(resolved)) {
					CommitStorage oldStorage = masters.get(id).getValue1();
					if (oldStorage instanceof CommitStorageRocksDb) {
						await(((CommitStorageRocksDb) oldStorage).stop());
					}
					deleteFolder(resolved);
				}
				storage = storageFn.apply(temporaryFolder.newFolder(folder).toPath());
				GlobalOTNodeImpl master = GlobalOTNodeImpl.create(Eventloop.getCurrentEventloop(),
						new RawServerId(folder),
						discoveryService,
						storage,
						createFactory())
						.withLatencyMargin(Duration.ZERO)
						.withCurrentTimeProvider(timeProvider)
						.withPollMasterRepositories(false);
				masters.put(id, new Tuple2<>(storage, master));
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		});
		AnnounceData announceData = new AnnounceData(now.currentTimeMillis(), masters.keySet().stream()
				.map(id -> new RawServerId("master" + id))
				.collect(toSet()));
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, PRIV_KEY);
		await(discoveryService.announce(PUB_KEY, signedData));
	}

	@BeforeClass
	public static void disableLogs() {
		enableLogging(SteppingCurrentTimeProvider.class, Level.WARN);
	}

	@Before
	public void initialize() throws IOException {
		discoveryService = LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), ANNOUNCEMENT_STORAGE, SHARED_KEY_STORAGE);
		masters.clear();
		turnedOffNodes.clear();
		COMMIT_ID = 1;
		intermediateStorage = storageFn.apply(temporaryFolder.newFolder("intermediate").toPath());
		intermediateNode = GlobalOTNodeImpl.create(Eventloop.getCurrentEventloop(),
				new RawServerId("intermediate"),
				discoveryService,
				intermediateStorage,
				createFactory())
				.withLatencyMargin(Duration.ZERO)
				.withCurrentTimeProvider(now)
				.withPollMasterRepositories(false);
		initializeMasters(1);
	}

	@Test
	public void testList() {
		GlobalOTNode masterNode = getMasterNode(1);
		addSingleCommit(emptySet(), masterNode);

		Set<String> listMaster = await(masterNode.list(PUB_KEY));
		assertEquals(1, listMaster.size());
		assertEquals("Test repository", first(listMaster));

		Set<String> intermediateList = await(intermediateNode.list(PUB_KEY));
		assertEquals(1, intermediateList.size());
		assertEquals("Test repository", first(intermediateList));
	}

	@Test
	public void testSaveRootCommit() {
		addSingleCommit(emptySet(), intermediateNode);
		assertCommits(getCommitId(1));
		assertHeads(getCommitId(1));
	}

	@Test
	public void testSaveRootCommitAndOtherCommits() {
		addCommits(5, intermediateNode);
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		assertCommits(commitIds);
		assertHeads(getCommitId(5));
	}

	@Test
	public void testSaveRootCommitAndAfterThatSaveOtherCommits() {
		addSingleCommit(emptySet(), intermediateNode);
		assertCommits(getCommitId(1));
		assertHeads(getCommitId(1));

		addCommits(1, 2, intermediateNode);

		assertCommits(getCommitId(1), getCommitId(2), getCommitId(3));
		assertHeads(getCommitId(3));
	}

	@Test
	public void testLoadCommitPresentOnIntermediate() {
		addCommits(5, intermediateNode);

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(3)));
		assertEquals(set(getCommitId(2)), commit.getParents());
	}

	@Test
	public void testLoadRootCommitNotPresentOnIntermediate() {
		addCommits(3, getMasterNode(1));

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(1)));
		assertEquals(emptySet(), commit.getParents());
	}

	@Test
	public void testLoadRandomCommitNotPresentOnIntermediate() {
		addCommits(3, getMasterNode(1));

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(3)));
		assertEquals(set(getCommitId(2)), commit.getParents());
	}

	@Test
	public void testSaveSnapshotOnMaster() {
		saveSnapshotsOn(getMasterNode(1), 5);

		assertSnapshots(getMasterStorage(1), 1, 2, 3, 4, 5);
	}

	@Test
	public void testSaveSnapshotOnIntermediate() {
		saveSnapshotsOn(intermediateNode, 5);

		assertSnapshots(1, 2, 3, 4, 5);
	}

	@Test
	public void testLoadSnapshotPresentOnIntermediate() {
		saveSnapshotsOn(intermediateNode, 1);

		Optional<SignedData<RawSnapshot>> snapshot = await(intermediateNode.loadSnapshot(REPO_ID, getCommitId(1)));
		assertTrue(snapshot.isPresent());
		assertSnapshots(1);
	}

	@Test
	public void testLoadSnapshotPresentOnMaster() {
		saveSnapshotsOn(getMasterNode(1), 1);

		Optional<SignedData<RawSnapshot>> snapshot = await(intermediateNode.loadSnapshot(REPO_ID, getCommitId(1)));
		assertTrue(snapshot.isPresent());
		assertSnapshots(1);
	}

	@Test
	public void testLoadNotPresentSnapshot() {
		Optional<SignedData<RawSnapshot>> snapshot = await(intermediateNode.loadSnapshot(REPO_ID, getCommitId(1)));
		assertFalse(snapshot.isPresent());
	}

	@Test
	public void testGetHeads() {
		GlobalOTNode masterNode = getMasterNode(1);
		addCommits(3, masterNode); // Single Head - 3
		addCommits(2, masterNode); // Heads - 3, 5

		Set<SignedData<RawCommitHead>> heads = await(intermediateNode.getHeads(REPO_ID));
		Set<CommitId> headsCommitIds = heads.stream().map(head -> head.getValue().getCommitId()).collect(toSet());
		CommitId commitId = getCommitId(2, 5);
		assertEquals(set(getCommitId(3, 3), commitId), headsCommitIds);
	}

	@Test
	public void testDownload() {
		GlobalOTNode globalOTNode = getMasterNode(1);
		addCommits(5, globalOTNode);
		int headCommitId = 5;

		Set<CommitId> heads = await(globalOTNode.getHeads(REPO_ID)).stream()
				.map(signedHeads -> signedHeads.getValue().getCommitId())
				.collect(toSet());
		assertEquals(set(getCommitId(headCommitId)), heads);

		ChannelSupplier<CommitEntry> commitSupplier = ChannelSupplier.ofPromise(globalOTNode.download(REPO_ID, heads));
		CommitEntry commitEntry;
		while ((commitEntry = await(commitSupplier.get())) != null) {
			assertEquals(getCommitId(headCommitId--), commitEntry.getCommitId());
		}
		assertEquals(0, headCommitId);
	}

	@Test
	public void testUpload() {
		List<CommitEntry> entries = new ArrayList<>();
		entries.add(createCommitEntry(emptySet()));             // id - 1
		entries.add(createCommitEntry(getCommitIds(1)));  // id - 2
		entries.add(createCommitEntry(getCommitIds(2)));  // id - 3
		entries.add(createCommitEntry(getCommitIds(3)));  // id - 4, head
		entries.add(createCommitEntry(getCommitIds(1)));            // id - 5
		entries.add(createCommitEntry(set(getCommitId(2, 5))));  // id - 6
		entries.add(createCommitEntry(set(getCommitId(3, 6))));  // id - 7, head

		Collections.sort(entries);

		CommitId head1 = getCommitId(4, 4);
		CommitId head2 = getCommitId(4, 7);

		Set<SignedData<RawCommitHead>> heads = Stream.of(head1, head2)
				.map(GlobalOTNodeImplTest::toSignedHead)
				.collect(toSet());

		await(ChannelSupplier.ofIterable(entries)
				.streamTo(ChannelConsumer.ofPromise(getMasterNode(1).upload(REPO_ID, heads))));
		CommitStorage masterStorage = getMasterStorage(1);
		assertHeads(masterStorage, head1, head2);
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4);
		commitIds.add(getCommitId(2, 5));
		commitIds.add(getCommitId(3, 6));
		commitIds.add(getCommitId(4, 7));
		assertCommits(masterStorage, commitIds);
	}

	@Test
	public void testUploadFromMasterToIntermediate() {
		GlobalOTNode masterNode = getMasterNode(1);
		addCommits(5, masterNode); //id - 1, 2, 3, 4, 5(head)
		addCommits(4, masterNode); //id - 6, 7, 8, 9 (head)

		Set<SignedData<RawCommitHead>> signedHeads = await(masterNode.getHeads(REPO_ID));
		Set<CommitId> heads = signedHeads.stream().map(signedHead -> signedHead.getValue().getCommitId()).collect(toSet());
		await(ChannelSupplier.ofPromise(masterNode.download(REPO_ID, heads))
				.streamTo(ChannelConsumer.ofPromise(intermediateNode.upload(REPO_ID, signedHeads))));

		assertHeads(getCommitId(5, 5), getCommitId(4, 9));
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		commitIds.add(getCommitId(1, 6));
		commitIds.add(getCommitId(2, 7));
		commitIds.add(getCommitId(3, 8));
		commitIds.add(getCommitId(4, 9));
		assertCommits(commitIds);
	}

	@Test
	public void testSendPullRequest() {
		RawPullRequest pullRequest = RawPullRequest.of(REPO_ID, RepoID.of(PUB_KEY, "Fork repository"));
		SignedData<RawPullRequest> signedData = SignedData.sign(REGISTRY.get(RawPullRequest.class), pullRequest, PRIV_KEY);

		await(intermediateNode.sendPullRequest(signedData));
		assertPullRequests(signedData);
	}

	@Test
	public void testGetPullRequest() {
		RawPullRequest pullRequest = RawPullRequest.of(REPO_ID, RepoID.of(PUB_KEY, "Fork repository"));
		SignedData<RawPullRequest> signedData = SignedData.sign(REGISTRY.get(RawPullRequest.class), pullRequest, PRIV_KEY);

		await(getMasterStorage(1).savePullRequest(signedData));
		Set<SignedData<RawPullRequest>> requests = await(intermediateNode.getPullRequests(REPO_ID));
		assertEquals(set(signedData), requests);
		assertPullRequests(signedData);
	}

	@Test
	public void testSync2Masters() {
		initializeMasters(2);
		CommitStorage firstMasterStorage = getMasterStorage(1);
		CommitStorage secondMasterStorage = getMasterStorage(2);
		GlobalOTNode firstMaster = getMasterNode(1);
		GlobalOTNode secondMaster = getMasterNode(2);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(5, intermediateNode);

		// Assume fetch iteration passed
		await(((GlobalOTNodeImpl) firstMaster).fetch());
		await(((GlobalOTNodeImpl) secondMaster).fetch());

		// Another fetch iteration just to be sure (because the order of fetches matters)
		await(((GlobalOTNodeImpl) firstMaster).fetch());
		await(((GlobalOTNodeImpl) secondMaster).fetch());

		CommitId head = getCommitId(5);
		assertHeads(intermediateStorage, head);
		assertHeads(firstMasterStorage, head);
		assertHeads(secondMasterStorage, head);
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		assertCommits(intermediateStorage, commitIds);
		assertCommits(firstMasterStorage, commitIds);
		assertCommits(secondMasterStorage, commitIds);
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testSyncRandomNumberOfMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(5, intermediateNode);

		// Assume fetch iteration passed
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).fetch());
		}

		// Another fetch iteration just to be sure (because the order of fetches matters)
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).fetch());
		}

		CommitId head = getCommitId(5);
		assertHeads(intermediateStorage, head);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), head);
		}

		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		assertCommits(intermediateStorage, commitIds);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), commitIds);
		}
	}

	@Test
	public void testCatchupMasters() {
		initializeMasters(2, () -> 10);
		CommitStorage firstMasterStorage = getMasterStorage(1);
		CommitStorage secondMasterStorage = getMasterStorage(2);
		GlobalOTNode firstMaster = getMasterNode(1);
		GlobalOTNode secondMaster = getMasterNode(2);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(5, intermediateNode);

		// Assume catch up iteration passed
		await(((GlobalOTNodeImpl) secondMaster).catchUp());
		await(((GlobalOTNodeImpl) firstMaster).catchUp());

		// Another catch up iteration just to be sure (because the order of fetches matters)
		await(((GlobalOTNodeImpl) firstMaster).catchUp());
		await(((GlobalOTNodeImpl) secondMaster).catchUp());

		CommitId head = getCommitId(5);
		assertHeads(intermediateStorage, head);
		assertHeads(firstMasterStorage, head);
		assertHeads(secondMasterStorage, head);
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		assertCommits(intermediateStorage, commitIds);
		assertCommits(firstMasterStorage, commitIds);
		assertCommits(secondMasterStorage, commitIds);
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testCatchupRandomNumberOfMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1, () -> 10);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(5, intermediateNode);

		// Assume catch up iteration passed
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).catchUp());
		}

		// Another catch up iteration just to be sure (because the order of fetches matters)
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).catchUp());
		}

		CommitId head = getCommitId(5);
		assertHeads(intermediateStorage, head);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), head);
		}

		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		assertCommits(intermediateStorage, commitIds);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), commitIds);
		}
	}

	@Test
	public void testPush2Masters() {
		initializeMasters(2);
		CommitStorage firstMasterStorage = getMasterStorage(1);
		CommitStorage secondMasterStorage = getMasterStorage(2);
		GlobalOTNode firstMaster = getMasterNode(1);
		GlobalOTNode secondMaster = getMasterNode(2);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(5, intermediateNode);

		// Assume push iteration passed
		await(((GlobalOTNodeImpl) secondMaster).push());
		await(((GlobalOTNodeImpl) firstMaster).push());

		CommitId head = getCommitId(5);
		assertHeads(intermediateStorage, head);
		assertHeads(firstMasterStorage, head);
		assertHeads(secondMasterStorage, head);
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		assertCommits(intermediateStorage, commitIds);
		assertCommits(firstMasterStorage, commitIds);
		assertCommits(secondMasterStorage, commitIds);
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testPushRandomNumberOfMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(5, intermediateNode);

		// Assume push iteration passed
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).push());
		}

		CommitId head = getCommitId(5);
		assertHeads(intermediateStorage, head);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), head);
		}

		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5);
		assertCommits(intermediateStorage, commitIds);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), commitIds);
		}
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testPushIntermediateToMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1);

		// ensuring pub key
		((GlobalOTNodeImpl) intermediateNode).ensureRepository(REPO_ID);

		intermediateStorage.saveCommit(getCommitId(1), RawCommit.of(0, emptySet(),
				EncryptedData.encrypt(new byte[]{1}, SIM_KEY), HASH, now.currentTimeMillis()));
		CommitId head = getCommitId(2);
		intermediateStorage.saveCommit(head, RawCommit.of(0, set(getCommitId(1)),
				EncryptedData.encrypt(new byte[]{1}, SIM_KEY), HASH, now.currentTimeMillis()));
		SignedData<RawCommitHead> signedHead = toSignedHead(head);
		intermediateStorage.updateHeads(REPO_ID, set(signedHead), emptySet());

		// wait for previous saves to complete
		await();

		// Assume pushed new commits
		await(((GlobalOTNodeImpl) intermediateNode).push());

		assertHeads(intermediateStorage, head);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), head);
		}

		Set<CommitId> commitIds = getCommitIds(1, 2);
		assertCommits(intermediateStorage, commitIds);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), commitIds);
		}
	}

	@Test
	public void testDownloadCommitNotPresentOnIntermediate() {
		GlobalOTNode masterNode = getMasterNode(1);
		int numberOfCommits, headCommit;
		numberOfCommits = headCommit = 5;
		addCommits(numberOfCommits, masterNode);

		ChannelSupplier<CommitEntry> commitSupplier = ChannelSupplier.ofPromise(
				intermediateNode.download(REPO_ID, getCommitIds(headCommit))
		);

		CommitEntry commitEntry;
		while ((commitEntry = await(commitSupplier.get())) != null) {
			assertEquals(getCommitId(headCommit--), commitEntry.getCommitId());
		}
		assertEquals(0, headCommit);
		Set<CommitId> commitIds = IntStream.range(1, numberOfCommits + 1).mapToObj(TestUtils::getCommitId).collect(toSet());
		assertCommits(intermediateStorage, commitIds);
	}

	@Test
	public void testUploadCommitsNotPresentOnMaster() {
		List<CommitEntry> entries = new ArrayList<>();
		entries.add(createCommitEntry(emptySet()));             // id - 1
		entries.add(createCommitEntry(getCommitIds(1)));  // id - 2
		entries.add(createCommitEntry(getCommitIds(2)));  // id - 3
		entries.add(createCommitEntry(getCommitIds(3)));  // id - 4, head
		entries.add(createCommitEntry(getCommitIds(1)));            // id - 5
		entries.add(createCommitEntry(set(getCommitId(2, 5))));  // id - 6
		entries.add(createCommitEntry(set(getCommitId(3, 6))));  // id - 7, head
		Collections.sort(entries);

		CommitId head1 = getCommitId(4, 4);
		CommitId head2 = getCommitId(4, 7);
		Set<SignedData<RawCommitHead>> heads = Stream.of(head1, head2)
				.map(GlobalOTNodeImplTest::toSignedHead)
				.collect(toSet());

		await(ChannelSupplier.ofIterable(entries)
				.streamTo(ChannelConsumer.ofPromise(intermediateNode.upload(REPO_ID, heads))));
		assertHeads(intermediateStorage, head1, head2);
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4);
		commitIds.add(getCommitId(2, 5));
		commitIds.add(getCommitId(3, 6));
		commitIds.add(getCommitId(4, 7));
		assertCommits(intermediateStorage, commitIds);
	}

	@Test
	public void testMasterGoingOffline() {
		((GlobalOTNodeImpl) intermediateNode).withRetryPolicy(RetryPolicy.noRetry());
		addCommits(3, intermediateNode); // commits with Ids 1, 2, 3

		turnOff("master1");
		GlobalOTNode masterNode = getMasterNode(1);
		assertTrue(masterNode instanceof FailingGlobalOTNode);

		addCommits(3, 3, intermediateNode); // commits with Ids 4, 5, 6

		// commits are still saved on local node
		Set<CommitId> localCommits = getCommitIds(1, 2, 3, 4, 5, 6);
		assertCommits(intermediateStorage, localCommits);

		CommitStorage masterStorage = getStorage(masterNode);
		// first 3 commits are saved on master node
		Set<CommitId> savedCommits = getCommitIds(1, 2, 3);
		assertCommits(masterStorage, savedCommits);

		// last 3 commits are not saved on master node
		Set<CommitId> newCommitIds = getCommitIds(4, 5, 6);
		assertCommitsAbsent(masterStorage, newCommitIds);

		// master is back online
		turnOn("master1");

		// assume more commits has been added to master while it was offline for intermediate node
		addSingleCommit(getCommitIds(3), getMasterNode(1)); // commit with Id 7
		CommitId newCommitId = getCommitId(4, 7);
		assertCommits(masterStorage, set(newCommitId)); // present on master
		assertCommitsAbsent(intermediateStorage, set(newCommitId)); // absent on intermediate

		// assume push iteration passed
		await(((GlobalOTNodeImpl) intermediateNode).push());
		// commits are now present on master node
		assertCommits(masterStorage, newCommitIds);

		// assume fetch iteration passed
		await(((GlobalOTNodeImpl) intermediateNode).fetch());
		// all commits are synced now
		Set<CommitId> commitIds = getCommitIds(1, 2, 3, 4, 5, 6);
		commitIds.add(newCommitId);
		assertCommits(commitIds);
	}

	@Test
	public void testPushSnapshots() {
		addCommits(3, intermediateNode);

		turnOff("master1");
		CommitId snapshotId = getCommitId(1);
		SignedData<RawSnapshot> snapshot = SignedData.sign(
				REGISTRY.get(RawSnapshot.class),
				RawSnapshot.of(REPO_ID, snapshotId, EncryptedData.encrypt(DATA, SIM_KEY), HASH),
				PRIV_KEY
		);

		await(intermediateNode.saveSnapshot(REPO_ID, snapshot));
		Set<CommitId> snapshotIds = await(intermediateStorage.listSnapshotIds(REPO_ID));
		assertEquals(1, snapshotIds.size());
		assertEquals(snapshotId, first(snapshotIds));

		CommitStorage masterStorage = getMasterStorage(1);
		Set<CommitId> masterSnapshotIds = await(masterStorage.listSnapshotIds(REPO_ID));
		assertEquals(0, masterSnapshotIds.size());

		turnOn("master1");
		await(((GlobalOTNodeImpl) intermediateNode).pushSnapshots());

		Set<CommitId> newSnapshotIds = await(masterStorage.listSnapshotIds(REPO_ID));
		assertEquals(1, newSnapshotIds.size());
		assertEquals(snapshotId, first(newSnapshotIds));
	}

	@Test
	public void testPushPullRequests() {
		addCommits(3, intermediateNode);

		turnOff("master1");
		RawPullRequest pullRequest = RawPullRequest.of(REPO_ID, RepoID.of(PUB_KEY, "Fork"));
		SignedData<RawPullRequest> signedPullRequest = SignedData.sign(
				REGISTRY.get(RawPullRequest.class),
				pullRequest,
				PRIV_KEY
		);
		await(intermediateNode.sendPullRequest(signedPullRequest));
		Set<SignedData<RawPullRequest>> snapshotIds = await(intermediateStorage.getPullRequests(REPO_ID));
		assertEquals(1, snapshotIds.size());
		assertEquals(signedPullRequest, first(snapshotIds));

		CommitStorage masterStorage = getMasterStorage(1);
		Set<SignedData<RawPullRequest>> masterSnapshotIds = await(masterStorage.getPullRequests(REPO_ID));
		assertEquals(0, masterSnapshotIds.size());

		turnOn("master1");
		await(((GlobalOTNodeImpl) intermediateNode).pushPullRequests());

		Set<SignedData<RawPullRequest>> newSnapshotIds = await(masterStorage.getPullRequests(REPO_ID));
		assertEquals(1, newSnapshotIds.size());
		assertEquals(signedPullRequest, first(newSnapshotIds));
	}

	@Test
	public void testDiscoveryServiceNoAnnounce() throws IOException {
		FailingDiscoveryService discoveryService = new FailingDiscoveryService() {
			@Override
			public Promise<@Nullable SignedData<AnnounceData>> find(PubKey space) {
				return Promise.of(null);
			}
		};
		GlobalOTNodeImpl node = GlobalOTNodeImpl.create(Eventloop.getCurrentEventloop(), new RawServerId("test"),
				discoveryService, storageFn.apply(temporaryFolder.newFolder().toPath()), id -> null);

		await(node.list(PUB_KEY));
	}

	// region helpers
	private CommitStorage getMasterStorage(Integer id) {
		return masters.get(id).getValue1();
	}

	private GlobalOTNode getMasterNode(Integer id) {
		return masters.get(id).getValue2();
	}

	private void saveSnapshotsOn(GlobalOTNode globalOTNode, int numberOfSnapshots) {
		Set<SignedData<RawSnapshot>> snapshots = new HashSet<>();
		for (int i = 0; i < numberOfSnapshots; i++) {
			snapshots.add(createSnapshot());
		}
		await(Promises.sequence(snapshots.stream()
				.map(snapshot -> () -> globalOTNode.saveSnapshot(REPO_ID, snapshot))));
	}

	private SignedData<RawSnapshot> createSnapshot() {
		RawSnapshot rawSnapshot = RawSnapshot.of(REPO_ID, nextCommitId(COMMIT_ID), EncryptedData.encrypt(DATA, SIM_KEY), Hash.sha1(SIM_KEY.getAesKey().getKey()));
		return SignedData.sign(REGISTRY.get(RawSnapshot.class), rawSnapshot, PRIV_KEY);
	}

	public static CommitEntry createCommitEntry(Set<CommitId> parents) {
		EncryptedData encryptedData = EncryptedData.encrypt(DATA, SIM_KEY);
		RawCommit rawCommit = RawCommit.of(0, parents, encryptedData, HASH, now.currentTimeMillis());
		CommitId commitId = nextCommitId(rawCommit.getLevel());
		return new CommitEntry(commitId, rawCommit);
	}

	private void addCommits(int numberOfCommits, GlobalOTNode node) {
		addCommits(0, numberOfCommits, node);
	}

	private void addCommits(Integer parentId, int numberOfCommits, GlobalOTNode node) {
		CommitStorage storage = getStorage(node);
		int parentLevel = parentId == 0 ? 0 : getParentsMaxLevel(storage, set(parentId));

		Set<CommitEntry> entries = new LinkedHashSet<>();
		for (int i = 0; i < numberOfCommits; i++) {
			CommitEntry commitEntry = createCommitEntry(parentId == 0 ? emptySet() : set(getCommitId(parentLevel, parentId)));
			parentId = COMMIT_ID - 1;
			parentLevel++;
			entries.add(commitEntry);
		}
		Map<CommitId, RawCommit> commits = entries.stream().collect(toMap(CommitEntry::getCommitId, CommitEntry::getCommit));
		Set<SignedData<RawCommitHead>> heads = singleton(toSignedHead(getCommitId(parentLevel, parentId)));

		await(node.saveAndUpdateHeads(REPO_ID, commits, heads));
	}

	private void addSingleCommit(Set<CommitId> parents, GlobalOTNode node) {
		CommitEntry commitEntry = createCommitEntry(parents);

		await(node.saveAndUpdateHeads(REPO_ID,
				map(commitEntry.getCommitId(), commitEntry.getCommit()),
				singleton(toSignedHead(commitEntry.getCommitId()))));
	}

	private CommitStorage getStorage(GlobalOTNode node) {
		return node.equals(intermediateNode) ?
				intermediateStorage :
				masters.values()
						.stream()
						.filter(tuple -> tuple.getValue2().equals(node))
						.map(Tuple2::getValue1)
						.findAny()
						.orElseThrow(AssertionError::new);
	}

	private static CommitId nextCommitId(long level) {
		CommitId commitId = getCommitId(level, COMMIT_ID);
		COMMIT_ID++;
		return commitId;
	}

	private static SignedData<RawCommitHead> toSignedHead(CommitId commitId) {
		return SignedData.sign(
				REGISTRY.get(RawCommitHead.class),
				RawCommitHead.of(REPO_ID, commitId, now.currentTimeMillis()),
				PRIV_KEY);
	}

	private Function<RawServerId, GlobalOTNode> createFactory() {
		return rawServerId -> {
			String idString = rawServerId.getServerIdString();

			if (idString.equals("intermediate")) {
				return intermediateNode;
			}
			if (idString.startsWith("master")) {
				Integer id = Integer.valueOf(idString.substring(6));
				return masters.get(id).getValue2();
			}
			throw new AssertionError("No server corresponds to this id: " + rawServerId.getServerIdString());
		};
	}

	private void assertSnapshots(int... ids) {
		assertSnapshots(getMasterStorage(1), ids);
		assertSnapshots(intermediateStorage, ids);
	}

	private void assertSnapshots(CommitStorage storage, int... ids) {
		List<Optional<SignedData<RawSnapshot>>> snapshots = await(Promises.toList(IntStream.of(ids).mapToObj(id -> storage.loadSnapshot(REPO_ID,
				getCommitId(id)))));
		Set<CommitId> snapshotIds = snapshots.stream().map(optional -> optional.orElseThrow(AssertionError::new).getValue().getCommitId()).collect(toSet());
		assertEquals(getCommitIds(ids), snapshotIds);

	}

	private void assertCommitsAbsent(Set<CommitId> ids) {
		assertCommitsAbsent(intermediateStorage, ids);
		assertCommitsAbsent(getMasterStorage(1), ids);
	}

	private void assertCommitsAbsent(CommitStorage storage, Set<CommitId> ids) {
		Boolean hasCommits = await(Promises.reduce(toAny(), Integer.MAX_VALUE, ids.stream().map(storage::hasCommit).iterator()));
		assertFalse(hasCommits);
	}

	private void assertCommits(CommitId... ids) {
		assertCommits(new HashSet<>(Arrays.asList(ids)));
	}

	private void assertCommits(Set<CommitId> ids) {
		assertCommits(intermediateStorage, ids);
		assertCommits(getMasterStorage(1), ids);
	}

	private void assertCommits(CommitStorage storage, Set<CommitId> ids) {
		Boolean hasCommits = await(Promises.reduce(toAll(), Integer.MAX_VALUE, ids.stream().map(storage::hasCommit).iterator()));
		assertTrue(hasCommits);
	}

	private void assertHeads(CommitId... ids) {
		assertHeads(intermediateStorage, ids);
		assertHeads(getMasterStorage(1), ids);
	}

	private void assertHeads(CommitStorage storage, CommitId... ids) {
		Map<CommitId, SignedData<RawCommitHead>> heads = await(storage.getHeads(REPO_ID));
		assertEquals(new HashSet<>(Arrays.asList(ids)), heads.keySet());
	}

	@SafeVarargs
	private final void assertPullRequests(SignedData<RawPullRequest>... pullRequests) {
		assertPullRequests(getMasterStorage(1), pullRequests);
		assertPullRequests(intermediateStorage, pullRequests);
	}

	@SafeVarargs
	private final void assertPullRequests(CommitStorage storage, SignedData<RawPullRequest>... pullRequests) {
		Set<SignedData<RawPullRequest>> requests = await(storage.getPullRequests(REPO_ID));
		assertEquals(set(pullRequests), requests);
	}

	private int getParentsMaxLevel(CommitStorage storage, Set<Integer> parents) {
		return await(
				Promises.toList(parents.stream()
						.map(id -> storage.loadCommit(getCommitId(id))))
						.map(parentCommits -> parentCommits.stream()
								.map(rawCommit -> rawCommit.orElseThrow(AssertionError::new).getLevel())
								.max(naturalOrder()).orElseThrow(AssertionError::new))
		).intValue();
	}

	private void turnOff(String idString) {
		if (idString.equals("intermediate")) {
			turnedOffNodes.put(idString, intermediateNode);
			intermediateNode = new FailingGlobalOTNode();
		} else if (idString.startsWith("master")) {
			Integer id = Integer.valueOf(idString.substring(6));
			GlobalOTNode node = masters.get(id).getValue2();
			turnedOffNodes.put(idString, node);
			Tuple2<CommitStorage, GlobalOTNode> failingTuple = new Tuple2<>(masters.get(id).getValue1(), new FailingGlobalOTNode());
			masters.put(id, failingTuple);
		} else {
			throw new AssertionError("No server corresponds to this id: " + idString);
		}
		if (intermediateNode instanceof GlobalOTNodeImpl) {
			((GlobalOTNodeImpl) intermediateNode).ensureNamespace(PUB_KEY).getMasters().clear();
		}
	}

	private void turnOn(String idString) {
		if (!turnedOffNodes.containsKey(idString)) {
			throw new AssertionError("Node " + idString + " is not turned off");
		} else if (idString.equals("intermediate")) {
			intermediateNode = turnedOffNodes.remove(idString);
		} else if (idString.startsWith("master")) {
			Integer id = Integer.valueOf(idString.substring(6));
			GlobalOTNode node = turnedOffNodes.remove(idString);
			Tuple2<CommitStorage, GlobalOTNode> failingTuple = new Tuple2<>(masters.get(id).getValue1(), node);
			masters.put(id, failingTuple);
		} else {
			throw new AssertionError("No server corresponds to this id: " + idString);
		}
		if (intermediateNode instanceof GlobalOTNodeImpl) {
			((GlobalOTNodeImpl) intermediateNode).ensureNamespace(PUB_KEY).getMasters().clear();
		}
	}

	public void clearDiscovery() {
		ANNOUNCEMENT_STORAGE.clear();
		SHARED_KEY_STORAGE.clear();
	}

	public static void deleteFolder(Path folder) throws IOException {
		List<Path> contents = Files.list(folder).collect(toList());
		for (Path content : contents) {
			if (Files.isDirectory(content)) {
				deleteFolder(content);
			} else {
				Files.delete(content);
			}
		}
		Files.delete(folder);
	}
	// endregion
}
