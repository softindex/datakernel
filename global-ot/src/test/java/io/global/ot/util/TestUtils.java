package io.global.ot.util;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.utils.TestAdd;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestSet;
import io.global.ot.api.CommitId;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.object;

public class TestUtils {
	private static final Random RANDOM = new Random();
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
}
