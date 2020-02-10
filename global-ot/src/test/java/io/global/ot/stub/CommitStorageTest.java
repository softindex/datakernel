package io.global.ot.stub;

import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.Hash;
import io.global.common.SimKey;
import io.global.common.api.EncryptedData;
import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;
import io.global.ot.server.CommitStorage;
import io.global.ot.server.CommitStorageRocksDb;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.promise.TestUtils.await;
import static io.global.ot.util.TestUtils.getCommitId;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyMap;
import static java.util.Comparator.naturalOrder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CommitStorageTest {
	public static final SimKey SIM_KEY = SimKey.generate();
	public static final byte[] DATA = {1};

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Parameter()
	public Function<Path, CommitStorage> storageFn;

	private CommitStorage storage;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{(Function<Path, CommitStorage>) path -> new CommitStorageStub()
				},
				new Object[]{(Function<Path, CommitStorage>) path -> {
					CommitStorageRocksDb rocksDb = CommitStorageRocksDb.create(
							newCachedThreadPool(),
							getCurrentEventloop(),
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
	public void testSaveRootCommit() {
		Boolean saved = await(saveCommit(1, emptyMap()));
		assertTrue(saved);

		Optional<RawCommit> maybeCommit = await(storage.loadCommit(getCommitId(1)));
		assertTrue(maybeCommit.isPresent());

		Boolean isComplete = await(storage.isCompleteCommit(getCommitId(1)));
		assertTrue(isComplete);
	}

	@Test
	public void testSaveCommitWithUnknownParents() {
		Boolean saved = await(saveCommit(5, map(1, 1)));
		assertTrue(saved);

		Optional<RawCommit> maybeCommit = await(storage.loadCommit(getCommitId(2, 5)));
		assertTrue(maybeCommit.isPresent());

		Boolean isComplete = await(storage.isCompleteCommit(getCommitId(2, 5)));
		assertFalse(isComplete);
	}

	@Test
	public void testMarkCompleteCommitsSingleCommit() {
		Boolean saved = await(saveCommit(1, emptyMap()));
		assertTrue(saved);

		await(storage.markCompleteCommits());

		Boolean isComplete = await(storage.isCompleteCommit(getCommitId(1)));
		assertTrue(isComplete);
	}

	@Test
	public void testMarkCompleteCommitsMultipleCommits() {
		Boolean saved1 = await(saveCommit(1, emptyMap()));
		Boolean saved2 = await(saveCommit(2, map(1, 1)));
		Boolean saved3 = await(saveCommit(3, map(2, 2)));
		assertTrue(saved1 && saved2 && saved3);

		await(storage.markCompleteCommits());

		Boolean isFirstCommitComplete = await(storage.isCompleteCommit(getCommitId(1)));
		Boolean isSecondCommitComplete = await(storage.isCompleteCommit(getCommitId(2)));
		Boolean isThirdCommitComplete = await(storage.isCompleteCommit(getCommitId(3)));

		assertTrue(isFirstCommitComplete && isSecondCommitComplete && isThirdCommitComplete);
	}

	@Test
	public void testMarkCompleteCommitsCompletedWithRoot() {
		Boolean saved1 = await(saveCommit(2, map(1, 1)));
		Boolean saved2 = await(saveCommit(3, map(2, 2)));
		Boolean saved3 = await(saveCommit(4, map(3, 3)));
		assertTrue(saved1 && saved2 && saved3);

		assertFalse(await(storage.isCompleteCommit(getCommitId(2))));
		assertFalse(await(storage.isCompleteCommit(getCommitId(3))));
		assertFalse(await(storage.isCompleteCommit(getCommitId(4))));

		// saving root
		assertTrue(await(saveCommit(1, emptyMap())));

		await(storage.markCompleteCommits());

		assertTrue(await(storage.isCompleteCommit(getCommitId(1))));
		assertTrue(await(storage.isCompleteCommit(getCommitId(2))));
		assertTrue(await(storage.isCompleteCommit(getCommitId(3))));
		assertTrue(await(storage.isCompleteCommit(getCommitId(4))));
	}

	private Promise<Boolean> saveCommit(int id, Map<Integer, Integer> parents) {
		int level = parents.values().stream().max(naturalOrder()).orElse(0) + 1;
		CommitId commitId = getCommitId(level, id);
		Set<CommitId> parentCommitIds = parents.entrySet().stream().map(entry -> getCommitId(entry.getKey(), entry.getValue())).collect(toSet());
		RawCommit rawCommit = RawCommit.of(0, parentCommitIds, EncryptedData.encrypt(DATA, SIM_KEY), Hash.sha1(SIM_KEY.getAesKey().getKey()), currentTimeMillis());
		return storage.saveCommit(commitId, rawCommit);
	}

}
