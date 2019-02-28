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
import io.datakernel.async.Promises;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner.DatakernelRunnerFactory;
import io.datakernel.stream.processor.LoggingRule.LoggerConfig;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.time.SteppingCurrentTimeProvider;
import io.datakernel.util.Tuple2;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.EncryptedData;
import io.global.common.api.NodeFactory;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.api.*;
import io.global.ot.api.GlobalOTNode.CommitEntry;
import io.global.ot.api.GlobalOTNode.HeadsInfo;
import io.global.ot.stub.CommitStorageStub;
import io.global.ot.util.FailingGlobalOTNode;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.*;
import static org.junit.Assert.*;

@SuppressWarnings("unused")
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(DatakernelRunnerFactory.class)
public class GlobalOTNodeImplTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
	private static byte COMMIT_ID;

	@Parameter()
	public Function<Path, CommitStorage> storageFn;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{(Function<Path, CommitStorage>) path -> new CommitStorageStub()
				},
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
						.withLatencyMargin(Duration.ZERO);
				master.now = now;
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
		io.datakernel.test.TestUtils.enableLogging(SteppingCurrentTimeProvider.class, Level.WARN);
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
				.withLatencyMargin(Duration.ZERO);
		((GlobalOTNodeImpl) intermediateNode).now = now;
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
		assertCommits(1);
		assertHeads(1);
	}

	@Test
	public void testSaveRootCommitAndOtherCommits() {
		addCommits(null, 5, intermediateNode);
		assertCommits(1, 2, 3, 4, 5);
		assertHeads(5);
	}

	@Test
	public void testSaveRootCommitAndAfterThatSaveOtherCommits() {
		addSingleCommit(emptySet(), intermediateNode);
		assertCommits(1);
		assertHeads(1);

		addCommits(1, 2, intermediateNode);
		assertCommits(1, 2, 3);
		assertHeads(3);
	}

	@Test
	public void testLoadCommitPresentOnIntermediate() {
		addCommits(null, 5, intermediateNode);

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(3)));
		assertEquals(set(getCommitId(2)), commit.getParents());
	}

	@Test
	public void testLoadRootCommitNotPresentOnIntermediate() {
		addCommits(null, 3, getMasterNode(1));

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(1)));
		assertEquals(emptySet(), commit.getParents());
	}

	@Test
	public void testLoadRandomCommitNotPresentOnIntermediate() {
		addCommits(null, 3, getMasterNode(1));

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(3)));
		assertEquals(set(getCommitId(2)), commit.getParents());
	}

	@Test
	public void testGetHeadsInfoFromMaster() {
		GlobalOTNode masterNode = getMasterNode(1);
		addCommits(null, 5, masterNode); // Head with id - 5
		addCommits(null, 4, masterNode); // Head with id - 9
		addCommits(3, 3, masterNode);    // Head with id - 12

		HeadsInfo headsInfo = await(intermediateNode.getHeadsInfo(REPO_ID));
		assertEquals(getCommitIds(5, 9, 12), headsInfo.getRequired());
	}

	@Test
	public void testGetHeadsInfoFromIntermediate() {
		addCommits(null, 5, intermediateNode); // Single Head with id - 5
		addCommits(null, 4, intermediateNode); // 2 Heads with ids - 5, 9
		addCommits(3, 3, intermediateNode); // 3 Heads with ids - 5, 9, 12

		HeadsInfo headsInfo = await(intermediateNode.getHeadsInfo(REPO_ID));
		assertEquals(getCommitIds(5, 9, 12), headsInfo.getExisting());

		addSingleCommit(set(5, 9, 12), intermediateNode); // Single head with id 13

		headsInfo = await(intermediateNode.getHeadsInfo(REPO_ID));
		assertEquals(set(getCommitId(13)), headsInfo.getExisting());
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
		addCommits(null, 3, masterNode); // Single Head - 3
		addCommits(null, 2, masterNode); // Heads - 3, 5

		Set<SignedData<RawCommitHead>> heads = await(intermediateNode.getHeads(REPO_ID));
		Set<CommitId> headsCommitIds = heads.stream().map(head -> head.getValue().commitId).collect(toSet());
		assertEquals(getCommitIds(3, 5), headsCommitIds);
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testDownload() {
		GlobalOTNode globalOTNode = getMasterNode(1);
		addCommits(null, 5, globalOTNode);

		List<CommitEntry> commitEntries = await(ChannelSupplier.ofPromise(globalOTNode.download(REPO_ID, set(getCommitId(15)), emptySet())).toList());
		Set<CommitId> heads = commitEntries.stream()
				.filter(CommitEntry::hasHead)
				.map(entry -> entry.getHead().getValue().getCommitId())
				.collect(toSet());
		Set<CommitId> commitIds = commitEntries.stream()
				.map(CommitEntry::getCommitId)
				.collect(toSet());

		assertEquals(set(getCommitId(5)), heads);
		assertEquals(getCommitIds(1, 2, 3, 4, 5), commitIds);
	}

	@Test
	public void testUpload() {
		List<CommitEntry> entries = new ArrayList<>();
		entries.add(createCommitEntry(emptySet(), 0, false));  // id - 1
		entries.add(createCommitEntry(set(1), 1, false));      // id - 2
		entries.add(createCommitEntry(set(2), 2, false));      // id - 3
		entries.add(createCommitEntry(set(3), 3, true));       // id - 4, head
		entries.add(createCommitEntry(emptySet(), 0, false));  // id - 5
		entries.add(createCommitEntry(set(1), 1, false));      // id - 6
		entries.add(createCommitEntry(set(2), 2, true));       // id - 7, head

		await(ChannelSupplier.ofIterable(entries).streamTo(ChannelConsumer.ofPromise(getMasterNode(1).upload(REPO_ID))));
		CommitStorage masterStorage = getMasterStorage(1);
		assertHeads(masterStorage, 4, 7);
		assertCommits(masterStorage, 1, 2, 3, 4, 5, 6, 7);
	}

	@Test
	public void testUploadFromMasterToIntermediate() {
		GlobalOTNode masterNode = getMasterNode(1);
		addCommits(null, 5, masterNode); //id - 1, 2, 3, 4, 5(head)
		addCommits(null, 4, masterNode); //id - 6, 7, 8, 9 (head)

		HeadsInfo headsInfoIntermediate = await(((GlobalOTNodeImpl) intermediateNode).getLocalHeadsInfo(REPO_ID));
		await(ChannelSupplier.ofPromise(masterNode.download(REPO_ID, headsInfoIntermediate.getRequired(), headsInfoIntermediate.getExisting()))
				.streamTo(ChannelConsumer.ofPromise(intermediateNode.upload(REPO_ID))));

		assertHeads(5, 9);
		assertCommits(1, 2, 3, 4, 5, 6, 7, 8, 9);
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
		addCommits(null, 5, intermediateNode);

		// Assume fetch iteration passed
		await(((GlobalOTNodeImpl) firstMaster).fetch());
		await(((GlobalOTNodeImpl) secondMaster).fetch());

		// Another fetch iteration just to be sure (because the order of fetches matters)
		await(((GlobalOTNodeImpl) firstMaster).fetch());
		await(((GlobalOTNodeImpl) secondMaster).fetch());

		assertHeads(intermediateStorage, 5);
		assertHeads(firstMasterStorage, 5);
		assertHeads(secondMasterStorage, 5);
		assertCommits(intermediateStorage, 1, 2, 3, 4, 5);
		assertCommits(firstMasterStorage, 1, 2, 3, 4, 5);
		assertCommits(secondMasterStorage, 1, 2, 3, 4, 5);
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testSyncRandomNumberOfMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(null, 5, intermediateNode);

		// Assume fetch iteration passed
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).fetch());
		}

		// Another fetch iteration just to be sure (because the order of fetches matters)
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).fetch());
		}

		assertHeads(intermediateStorage, 5);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), 5);
		}

		assertCommits(intermediateStorage, 1, 2, 3, 4, 5);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), 1, 2, 3, 4, 5);
		}
	}

	@Test
	public void testCatchupMasters() {
		initializeMasters(2);
		masters.forEach((integer, tuple) -> ((GlobalOTNodeImpl) tuple.getValue2()).now = () -> 10);
		CommitStorage firstMasterStorage = getMasterStorage(1);
		CommitStorage secondMasterStorage = getMasterStorage(2);
		GlobalOTNode firstMaster = getMasterNode(1);
		GlobalOTNode secondMaster = getMasterNode(2);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(null, 5, intermediateNode);

		// Assume catch up iteration passed
		await(((GlobalOTNodeImpl) secondMaster).catchUp());
		await(((GlobalOTNodeImpl) firstMaster).catchUp());

		// Another catch up iteration just to be sure (because the order of fetches matters)
		await(((GlobalOTNodeImpl) firstMaster).catchUp());
		await(((GlobalOTNodeImpl) secondMaster).catchUp());

		assertHeads(intermediateStorage, 5);
		assertHeads(firstMasterStorage, 5);
		assertHeads(secondMasterStorage, 5);
		assertCommits(intermediateStorage, 1, 2, 3, 4, 5);
		assertCommits(firstMasterStorage, 1, 2, 3, 4, 5);
		assertCommits(secondMasterStorage, 1, 2, 3, 4, 5);
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testCatchupRandomNumberOfMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1);
		masters.forEach((integer, tuple) -> ((GlobalOTNodeImpl) tuple.getValue2()).now = () -> 10);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(null, 5, intermediateNode);

		// Assume catch up iteration passed
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).catchUp());
		}

		// Another catch up iteration just to be sure (because the order of fetches matters)
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).catchUp());
		}

		assertHeads(intermediateStorage, 5);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), 5);
		}

		assertCommits(intermediateStorage, 1, 2, 3, 4, 5);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), 1, 2, 3, 4, 5);
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
		addCommits(null, 5, intermediateNode);

		// Assume push iteration passed
		await(((GlobalOTNodeImpl) secondMaster).push());
		await(((GlobalOTNodeImpl) firstMaster).push());

		assertHeads(intermediateStorage, 5);
		assertHeads(firstMasterStorage, 5);
		assertHeads(secondMasterStorage, 5);
		assertCommits(intermediateStorage, 1, 2, 3, 4, 5);
		assertCommits(firstMasterStorage, 1, 2, 3, 4, 5);
		assertCommits(secondMasterStorage, 1, 2, 3, 4, 5);
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testPushRandomNumberOfMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1);

		// will propagate commits to one master (Promises.firstSuccessfull())
		addCommits(null, 5, intermediateNode);

		// Assume push iteration passed
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			await(((GlobalOTNodeImpl) tuple.getValue2()).push());
		}

		assertHeads(intermediateStorage, 5);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), 5);
		}

		assertCommits(intermediateStorage, 1, 2, 3, 4, 5);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), 1, 2, 3, 4, 5);
		}
	}

	@Test
	@LoggerConfig(value = "WARN") // too many logs
	public void testPushIntermediateToMasters() {
		initializeMasters(RANDOM.nextInt(5) + 1);

		// ensuring pub key
		((GlobalOTNodeImpl) intermediateNode).ensureRepository(REPO_ID);

		intermediateStorage.saveCommit(getCommitId(1), RawCommit.of(emptySet(),
				EncryptedData.encrypt(new byte[]{1}, SIM_KEY), HASH, 1, now.currentTimeMillis()));
		intermediateStorage.saveCommit(getCommitId(2), RawCommit.of(set(getCommitId(1)),
				EncryptedData.encrypt(new byte[]{1}, SIM_KEY), HASH, 2, now.currentTimeMillis()));
		SignedData<RawCommitHead> signedHead = SignedData.sign(REGISTRY.get(RawCommitHead.class),
				RawCommitHead.of(REPO_ID, getCommitId(2), now.currentTimeMillis()), PRIV_KEY);
		intermediateStorage.updateHeads(REPO_ID, set(signedHead), emptySet());

		// wait for previous saves to complete
		await();

		// Assume pushed new commits
		await(((GlobalOTNodeImpl) intermediateNode).push());

		assertHeads(intermediateStorage, 2);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertHeads(tuple.getValue1(), 2);
		}

		assertCommits(intermediateStorage, 1, 2);
		for (Tuple2<CommitStorage, GlobalOTNode> tuple : masters.values()) {
			assertCommits(tuple.getValue1(), 1, 2);
		}
	}

	@Test
	public void testDownloadCommitNotPresentOnIntermediate() {
		GlobalOTNode masterNode = getMasterNode(1);
		addCommits(null, 5, masterNode);

		List<CommitId> downloadedCommits = await(ChannelSupplier.ofPromise(intermediateNode.download(REPO_ID, getCommitIds(5), emptySet())).toList())
				.stream().map(CommitEntry::getCommitId).collect(toList());

		IntStream.of(1, 2, 3, 4, 5).boxed().map(GlobalOTNodeImplTest::getCommitId).forEach(id -> assertTrue(downloadedCommits.contains(id)));
		assertCommits(intermediateStorage, 1, 2, 3, 4, 5);
	}

	@Test
	public void testUploadCommitsNotPresentOnMaster() {
		List<CommitEntry> entries = new ArrayList<>();
		entries.add(createCommitEntry(emptySet(), 0, false));  // id - 1
		entries.add(createCommitEntry(set(1), 1, false));      // id - 2
		entries.add(createCommitEntry(set(2), 2, false));      // id - 3
		entries.add(createCommitEntry(set(3), 3, true));       // id - 4, head
		entries.add(createCommitEntry(emptySet(), 0, false));  // id - 5
		entries.add(createCommitEntry(set(1), 1, false));      // id - 6
		entries.add(createCommitEntry(set(2), 2, true));       // id - 7, head

		await(ChannelSupplier.ofIterable(entries).streamTo(ChannelConsumer.ofPromise(intermediateNode.upload(REPO_ID))));
		assertHeads(4, 7);
		assertCommits(1, 2, 3, 4, 5, 6, 7);
	}

	@Test
	public void testMasterGoingOffline() {
		addCommits(null, 3, intermediateNode); // commits with Ids 1, 2, 3

		turnOff("master1");
		GlobalOTNode masterNode = getMasterNode(1);
		assertTrue(masterNode instanceof FailingGlobalOTNode);

		addCommits(null, 3, intermediateNode); // commits with Ids 4, 5, 6

		// commits are still saved on local node
		assertCommits(intermediateStorage, 1, 2, 3, 4, 5, 6);

		CommitStorage masterStorage = getStorage(masterNode);
		// first 3 commits are saved on master node
		assertCommits(masterStorage, 1, 2, 3);

		// last 3 commits are not saved on master node
		assertCommitsAbsent(masterStorage, 4, 5, 6);

		// master is back online
		turnOn("master1");

		// assume more commits has been added to master while it was offline for intermediate node
		addSingleCommit(set(3), getMasterNode(1)); // commit with Id 7
		assertCommits(masterStorage, 7); // present on master
		assertCommitsAbsent(intermediateStorage, 7); // absent on intermediate

		// assume push iteration passed
		await(((GlobalOTNodeImpl) intermediateNode).push());
		// commits are now present on master node
		assertCommits(masterStorage, 4, 5, 6);

		// assume fetch iteration passed
		await(((GlobalOTNodeImpl) intermediateNode).fetch());
		// all commits are synced now
		assertCommits(1, 2, 3, 4, 5, 6, 7);
	}

	@Test
	public void testPushSnapshots() {
		addCommits(null, 3, intermediateNode);

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
		addCommits(null, 3, intermediateNode);

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
		RawSnapshot rawSnapshot = RawSnapshot.of(REPO_ID, nextCommitId(), EncryptedData.encrypt(DATA, SIM_KEY), Hash.sha1(SIM_KEY.getAesKey().getKey()));
		return SignedData.sign(REGISTRY.get(RawSnapshot.class), rawSnapshot, PRIV_KEY);
	}

	public static CommitEntry createCommitEntry(Set<Integer> parents, long parentLevel, boolean head) {
		EncryptedData encryptedData = EncryptedData.encrypt(DATA, SIM_KEY);
		Set<CommitId> parentIds = parents.stream().map(GlobalOTNodeImplTest::getCommitId).collect(toSet());
		RawCommit rawCommit = RawCommit.of(parentIds, encryptedData, HASH, (parentLevel + 1), now.currentTimeMillis());
		CommitId commitId = nextCommitId();
		return new CommitEntry(commitId, rawCommit, head ?
				SignedData.sign(
						REGISTRY.get(RawCommitHead.class),
						RawCommitHead.of(
								REPO_ID,
								commitId,
								rawCommit.getTimestamp()),
						PRIV_KEY) :
				null);
	}

	private void addCommits(@Nullable Integer parentId, int numberOfCommits, GlobalOTNode node) {
		CommitStorage storage = getStorage(node);
		CommitId parent = parentId == null ? null : getCommitId(parentId);
		long parentLevel = parent == null ? 0 : getParentsMaxLevel(storage, set(parentId));

		Set<CommitEntry> entries = new LinkedHashSet<>();
		Set<Integer> parents = parent == null ? emptySet() : set(parentId);
		for (int i = 0; i < numberOfCommits; i++) {
			CommitEntry commitEntry = createCommitEntry(parents, parentLevel + i, i == numberOfCommits - 1);
			entries.add(commitEntry);
			parents = singleton((int) COMMIT_ID - 1);
		}
		Map<CommitId, RawCommit> commits = entries.stream().collect(toMap(CommitEntry::getCommitId, CommitEntry::getCommit));
		Set<SignedData<RawCommitHead>> heads = entries.stream().filter(CommitEntry::hasHead).map(CommitEntry::getHead).collect(toSet());

		await(node.save(REPO_ID, commits, heads));
	}

	private void addSingleCommit(Set<Integer> parents, GlobalOTNode node, long level) {
		CommitStorage storage = getStorage(node);
		long parentLevel = level == -1 ?
				parents.isEmpty() ? 0 : getParentsMaxLevel(storage, parents) :
				level;
		CommitEntry commitEntry = createCommitEntry(parents, parentLevel, true);

		assert commitEntry.getHead() != null;
		await(node.save(REPO_ID, commitEntry.getCommit(), commitEntry.getHead()));
	}

	private void addSingleCommit(Set<Integer> parents, GlobalOTNode node) {
		addSingleCommit(parents, node, -1);
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

	private static CommitId nextCommitId() {
		CommitId commitId = CommitId.ofBytes(new byte[]{COMMIT_ID});
		COMMIT_ID++;
		return commitId;
	}

	private static CommitId getCommitId(int id) {
		return CommitId.ofBytes(new byte[]{(byte) id});
	}

	private Set<CommitId> getCommitIds(int... ids) {
		Set<CommitId> commitIds = new HashSet<>();
		for (int id : ids) {
			commitIds.add(getCommitId(id));
		}
		return commitIds;
	}

	private NodeFactory<GlobalOTNode> createFactory() {
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
		List<Optional<SignedData<RawSnapshot>>> snapshots = await(Promises.toList(IntStream.of(ids).mapToObj(id -> storage.loadSnapshot(REPO_ID, getCommitId(id)))));
		Set<CommitId> snapshotIds = snapshots.stream().map(optional -> optional.orElseThrow(AssertionError::new).getValue().getCommitId()).collect(toSet());
		assertEquals(getCommitIds(ids), snapshotIds);

	}

	private void assertCommitsAbsent(int... ids) {
		assertCommits(intermediateStorage, ids);
		assertCommits(getMasterStorage(1), ids);
	}

	private void assertCommitsAbsent(CommitStorage storage, int... ids) {
		List<Boolean> hasCommits = await(Promises.toList(IntStream.of(ids).mapToObj(id -> storage.hasCommit(getCommitId(id)))));
		hasCommits.forEach(Assert::assertFalse);
	}

	private void assertCommits(int... ids) {
		assertCommits(intermediateStorage, ids);
		assertCommits(getMasterStorage(1), ids);
	}

	private void assertCommits(CommitStorage storage, int... ids) {
		List<Boolean> hasCommits = await(Promises.toList(IntStream.of(ids).mapToObj(id -> storage.hasCommit(getCommitId(id)))));
		hasCommits.forEach(Assert::assertTrue);
	}

	private void assertHeads(int... ids) {
		assertHeads(intermediateStorage, ids);
		assertHeads(getMasterStorage(1), ids);
	}

	private void assertHeads(CommitStorage storage, int... ids) {
		Map<CommitId, SignedData<RawCommitHead>> heads = await(storage.getHeads(REPO_ID));
		assertEquals(getCommitIds(ids), heads.keySet());
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

	private long getParentsMaxLevel(CommitStorage storage, Set<Integer> parents) {
		return await(
				Promises.toList(parents.stream()
						.map(id -> storage.loadCommit(getCommitId(id))))
						.thenApply(parentCommits -> parentCommits.stream()
								.map(rawCommit -> rawCommit.orElseThrow(AssertionError::new).getLevel())
								.max(naturalOrder()).orElseThrow(AssertionError::new))
		);
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
			((GlobalOTNodeImpl) intermediateNode).ensurePubKey(PUB_KEY).masterNodes.clear();
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
			((GlobalOTNodeImpl) intermediateNode).ensurePubKey(PUB_KEY).masterNodes.clear();
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
