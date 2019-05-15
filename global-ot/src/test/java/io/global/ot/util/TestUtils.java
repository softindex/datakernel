package io.global.ot.util;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.utils.TestAdd;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestSet;
import io.global.common.Hash;
import io.global.common.SimKey;
import io.global.common.api.EncryptedData;
import io.global.ot.api.CommitEntry;
import io.global.ot.api.CommitId;
import io.global.ot.api.RawCommit;

import java.util.*;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.ot.util.HttpDataFormats.COMMIT_CODEC;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

public class TestUtils {
	private static final Random RANDOM = new Random();
	private static final SimKey SIM_KEY = SimKey.generate();

	public static final StructuredCodec<TestOp> TEST_OP_CODEC = CodecSubtype.<TestOp>create()
			.with(TestAdd.class, object(TestAdd::new,
					"delta", TestAdd::getDelta, INT_CODEC))
			.with(TestSet.class, object(TestSet::new,
					"prev", TestSet::getPrev, INT_CODEC,
					"next", TestSet::getNext, INT_CODEC));

	private TestUtils() {
		throw new AssertionError();
	}

	public static CommitId getCommitId(int seed) {
		return getCommitId(seed, seed);
	}

	public static CommitId getCommitId(long level, int seed) {
		byte[] bytes = new byte[32];
		RANDOM.setSeed(seed);
		RANDOM.nextBytes(bytes);
		return CommitId.of(level, bytes);
	}

	public static Set<CommitId> getCommitIds(int... ids) {
		Set<CommitId> commitIds = new HashSet<>();
		for (int id : ids) {
			commitIds.add(getCommitId(id));
		}
		return commitIds;
	}

	public static List<CommitEntry> getCommitEntries(int size) {
		if (size < 0) return emptyList();
		List<CommitEntry> entries = new ArrayList<>();

		CommitEntry first = nextCommitEntry(singleton(CommitId.ofRoot()));
		entries.add(first);

		for (int i = 0; i < size; i++) {
			CommitEntry prev = entries.get(entries.size() - 1);
			CommitEntry next = nextCommitEntry(singleton(prev.getCommitId()));
			entries.add(next);
		}

		Collections.reverse(entries);
		return entries;
	}

	private static CommitEntry nextCommitEntry(Set<CommitId> parents) {
		RawCommit rawCommit = RawCommit.of(0, parents, EncryptedData.encrypt(new byte[]{1}, SIM_KEY),
				Hash.sha1(SIM_KEY.getBytes()), 0);
		long level = parents.stream().mapToLong(CommitId::getLevel).max().orElse(0) + 1L;
		return new CommitEntry(CommitId.ofCommitData(level, encodeAsArray(COMMIT_CODEC, rawCommit)), rawCommit);
	}

}
