package io.global.ot.stub;

import io.datakernel.async.Promise;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.Hash;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.common.api.EncryptedData;
import io.global.ot.api.RawCommit;
import io.global.ot.server.CommitStorage;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.Set;

import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.TestUtils.await;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
@DatakernelRunner.SkipEventloopRun
public class CommitStorageStubTest {
	public static final SimKey SIM_KEY = SimKey.generate();
	public static final byte[] DATA = {1};
	CommitStorage storage = new CommitStorageStub();

	@Test
	public void testSaveRootCommit() {
		Boolean saved = await(saveCommit(1, emptySet(), 1));
		assertTrue(saved);

		Optional<RawCommit> maybeCommit = await(storage.loadCommit(getCommitId(1)));
		assertTrue(maybeCommit.isPresent());

		Boolean isComplete = await(storage.isCompleteCommit(getCommitId(1)));
		assertTrue(isComplete);
	}

	@Test
	public void testSaveCommitWithUnknownParents() {
		Boolean saved = await(saveCommit(5, singleton(1), 2));
		assertTrue(saved);

		Optional<RawCommit> maybeCommit = await(storage.loadCommit(getCommitId(5)));
		assertTrue(maybeCommit.isPresent());

		Boolean isComplete = await(storage.isCompleteCommit(getCommitId(5)));
		assertFalse(isComplete);
	}

	@Test
	public void testMarkCompleteCommitsSingleCommit() {
		Boolean saved = await(saveCommit(1, emptySet(), 1));
		assertTrue(saved);

		await(storage.markCompleteCommits());

		Boolean isComplete = await(storage.isCompleteCommit(getCommitId(1)));
		assertTrue(isComplete);
	}

	@Test
	public void testMarkCompleteCommitsMultipleCommits() {
		Boolean saved1 = await(saveCommit(1, emptySet(), 1));
		Boolean saved2 = await(saveCommit(2, set(1), 2));
		Boolean saved3 = await(saveCommit(3, set(2), 3));
		assertTrue(saved1 && saved2 && saved3);

		await(storage.markCompleteCommits());

		Boolean isFirstCommitComplete = await(storage.isCompleteCommit(getCommitId(1)));
		Boolean isSecondCommitComplete = await(storage.isCompleteCommit(getCommitId(2)));
		Boolean isThirdCommitComplete = await(storage.isCompleteCommit(getCommitId(3)));

		assertTrue(isFirstCommitComplete && isSecondCommitComplete && isThirdCommitComplete);
	}

	private Promise<Boolean> saveCommit(int id, Set<Integer> parents, long level) {
		CommitId commitId = CommitId.ofBytes(new byte[]{(byte) id});
		Set<CommitId> parentIds = parents.stream().map(this::getCommitId).collect(toSet());
		RawCommit rawCommit = RawCommit.of(parentIds, EncryptedData.encrypt(DATA, SIM_KEY), Hash.sha1(SIM_KEY.getAesKey().getKey()), level, currentTimeMillis());
		return storage.saveCommit(commitId, rawCommit);
	}

	private CommitId getCommitId(int id) {
		return CommitId.ofBytes(new byte[]{(byte) id});
	}

}
