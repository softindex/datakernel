package io.global.ot.server;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promises;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.EncryptedData;
import io.global.common.api.NodeFactory;
import io.global.common.stub.DiscoveryServiceStub;
import io.global.ot.api.*;
import io.global.ot.api.GlobalOTNode.CommitEntry;
import io.global.ot.api.GlobalOTNode.HeadsInfo;
import io.global.ot.stub.CommitStorageStub;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.util.CollectionUtils.*;
import static io.global.common.TestUtils.await;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
@DatakernelRunner.SkipEventloopRun
public class GlobalOTNodeImplTest {
	private static final DiscoveryService DISCOVERY_SERVICE = new DiscoveryServiceStub();
	private static final KeyPair KEYS = KeyPair.generate();
	private static final PubKey PUB_KEY = KEYS.getPubKey();
	private static final PrivKey PRIV_KEY = KEYS.getPrivKey();
	private static final SimKey SIM_KEY = SimKey.generate();
	public static final String REPOSITORY_NAME = "Test repository";
	private static final RepoID REPO_ID = RepoID.of(PUB_KEY, REPOSITORY_NAME);
	private static final byte[] DATA = {1, 2, 3, 4, 5, 6};

	private CommitStorage masterStorage;
	private CommitStorage intermediateStorage;
	private GlobalOTNode masterNode;
	private GlobalOTNode intermediateNode;
	private byte COMMIT_ID;

	@BeforeClass
	public static void announce() throws ExecutionException, InterruptedException {
		AnnounceData announceData = new AnnounceData(1, singleton(new RawServerId("master")));
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, PRIV_KEY);
		DISCOVERY_SERVICE.announce(PUB_KEY, signedData).toCompletableFuture().get();
	}

	@Before
	public void reset() {
		COMMIT_ID = 1;
		masterStorage = new CommitStorageStub();
		intermediateStorage = new CommitStorageStub();
		masterNode = GlobalOTNodeImpl.create(Eventloop.getCurrentEventloop(), new RawServerId("master"), DISCOVERY_SERVICE, masterStorage, createFactory())
				.withManagedPubKeys(singleton(PUB_KEY));
		intermediateNode = GlobalOTNodeImpl.create(Eventloop.getCurrentEventloop(), new RawServerId("intermediate"), DISCOVERY_SERVICE, intermediateStorage, createFactory());
	}

	@Test
	public void testList() {
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
		addCommits(null, 3, masterNode);

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(1)));
		assertEquals(emptySet(), commit.getParents());
	}

	@Test
	public void testLoadRandomCommitNotPresentOnIntermediate() {
		addCommits(null, 3, masterNode);

		RawCommit commit = await(intermediateNode.loadCommit(REPO_ID, getCommitId(3)));
		assertEquals(set(getCommitId(2)), commit.getParents());
	}

	@Test
	public void testGetHeadsInfoFromMaster() {
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
		saveSnapshotsOn(masterNode, 5);

		assertSnapshots(masterStorage, 1, 2, 3, 4, 5);
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
		saveSnapshotsOn(masterNode, 1);

		Optional<SignedData<RawSnapshot>> snapshot = await(intermediateNode.loadSnapshot(REPO_ID, getCommitId(1)));
		assertTrue(snapshot.isPresent());
		assertSnapshots(1);
	}

	@Test
	public void testGetHeads() {
		addCommits(null, 3, masterNode); // Single Head - 3
		addCommits(null, 2, masterNode); // Heads - 3, 5

		Set<SignedData<RawCommitHead>> heads = await(intermediateNode.getHeads(REPO_ID));
		Set<CommitId> headsCommitIds = heads.stream().map(head -> head.getValue().commitId).collect(toSet());
		assertEquals(getCommitIds(3, 5), headsCommitIds);
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testDownload() {
		addCommits(null, 5, masterNode);

		HeadsInfo headsInfo = await(masterNode.getHeadsInfo(REPO_ID));

		List<CommitEntry> commitEntries = await(masterNode.downloader(REPO_ID, set(getCommitId(15)), emptySet()).toList());
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

		await(ChannelSupplier.ofIterable(entries).streamTo(masterNode.uploader(REPO_ID)));
		assertHeads(masterStorage, 4, 7);
		assertCommits(masterStorage, 1, 2, 3, 4, 5, 6, 7);
	}

	@Test
	public void testUploadFromMasterToIntermediate() {
		addCommits(null, 5, masterNode); //id - 1, 2, 3, 4, 5(head)
		addCommits(null, 4, masterNode); //id - 6, 7, 8, 9 (head)

		HeadsInfo headsInfoMaster = await(masterNode.getHeadsInfo(REPO_ID));
		HeadsInfo headsInfoIntermediate = await(intermediateNode.getHeadsInfo(REPO_ID));
		await(masterNode.downloader(REPO_ID, union(headsInfoMaster.getExisting(), headsInfoIntermediate.getRequired()), headsInfoIntermediate.getExisting())
				.streamTo(intermediateNode.uploader(REPO_ID)));

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

		await(masterStorage.savePullRequest(signedData));
		Set<SignedData<RawPullRequest>> requests = await(intermediateNode.getPullRequests(REPO_ID));
		assertEquals(set(signedData), requests);
		assertPullRequests(signedData);
	}

	// region helpers
	private void saveSnapshotsOn(GlobalOTNode globalOTNode, int numberOfSnapshots) {
		Set<SignedData<RawSnapshot>> snapshots = new HashSet<>();
		for (int i = 0; i < numberOfSnapshots; i++) {
			snapshots.add(createSnapshot());
		}
		await(Promises.all(snapshots.stream()
				.map(snapshot -> globalOTNode.saveSnapshot(REPO_ID, snapshot))));
	}

	private SignedData<RawSnapshot> createSnapshot() {
		RawSnapshot rawSnapshot = RawSnapshot.of(REPO_ID, nextCommitId(), EncryptedData.encrypt(DATA, SIM_KEY), Hash.sha1(SIM_KEY.getAesKey().getKey()));
		return SignedData.sign(REGISTRY.get(RawSnapshot.class), rawSnapshot, PRIV_KEY);
	}

	private CommitEntry createCommitEntry(Set<Integer> parents, long parentLevel, boolean head) {
		EncryptedData encryptedData = EncryptedData.encrypt(DATA, SIM_KEY);
		Set<CommitId> parentIds = parents.stream().map(this::getCommitId).collect(toSet());
		RawCommit rawCommit = RawCommit.of(parentIds, encryptedData, Hash.sha1(SIM_KEY.getAesKey().getKey()), (parentLevel + 1), currentTimeMillis());
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
		CommitStorage storage = node.equals(masterNode) ? masterStorage : intermediateStorage;
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

	private void addSingleCommit(Set<Integer> parents, GlobalOTNode node) {
		CommitStorage storage = node.equals(masterNode) ? masterStorage : intermediateStorage;
		long parentLevel = parents.isEmpty() ? 0 : getParentsMaxLevel(storage, parents);
		CommitEntry commitEntry = createCommitEntry(parents, parentLevel, true);

		assert commitEntry.getHead() != null;
		await(node.save(REPO_ID, commitEntry.getCommit(), commitEntry.getHead()));
	}

	private CommitId nextCommitId() {
		CommitId commitId = CommitId.ofBytes(new byte[]{COMMIT_ID});
		COMMIT_ID++;
		return commitId;
	}

	private CommitId getCommitId(int id) {
		return CommitId.ofBytes(new byte[]{(byte) id});
	}

	private Collection<CommitId> getCommitIds(int... ids) {
		Set<CommitId> commitIds = new HashSet<>();
		for (int id : ids) {
			commitIds.add(getCommitId(id));
		}
		return commitIds;
	}

	private NodeFactory<GlobalOTNode> createFactory() {
		return rawServerId -> {
			if (rawServerId.getServerIdString().equals("master")) {
				return masterNode;
			}
			if (rawServerId.getServerIdString().equals("intermediate")) {
				return intermediateNode;
			}
			throw new AssertionError("No server corresponds to this id: " + rawServerId.getServerIdString());
		};
	}

	private void assertSnapshots(int... ids) {
		assertSnapshots(masterStorage, ids);
		assertSnapshots(intermediateStorage, ids);
	}

	private void assertSnapshots(CommitStorage storage, int... ids) {
		List<Optional<SignedData<RawSnapshot>>> snapshots = await(Promises.toList(IntStream.of(ids).mapToObj(id -> storage.loadSnapshot(REPO_ID, getCommitId(id)))));
		Set<CommitId> snapshotIds = snapshots.stream().map(optional -> optional.orElseThrow(AssertionError::new).getValue().getCommitId()).collect(toSet());
		assertEquals(getCommitIds(ids), snapshotIds);

	}

	private void assertCommits(int... ids) {
		assertCommits(intermediateStorage, ids);
		assertCommits(masterStorage, ids);
	}

	private void assertCommits(CommitStorage storage, int... ids) {
		List<Boolean> hasCommits = await(Promises.toList(IntStream.of(ids).mapToObj(id -> storage.hasCommit(getCommitId(id)))));
		hasCommits.forEach(Assert::assertTrue);
	}

	private void assertHeads(int... ids) {
		assertHeads(intermediateStorage, ids);
		assertHeads(masterStorage, ids);
	}

	private void assertHeads(CommitStorage storage, int... ids) {
		Map<CommitId, SignedData<RawCommitHead>> heads = await(storage.getHeads(REPO_ID));
		assertEquals(getCommitIds(ids), heads.keySet());
	}

	@SafeVarargs
	private final void assertPullRequests(SignedData<RawPullRequest>... pullRequests) {
		assertPullRequests(masterStorage, pullRequests);
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
	// endregion
}
