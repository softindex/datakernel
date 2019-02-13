package io.global.ot.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.ot.utils.TestAdd;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestSet;
import io.global.ot.api.CommitId;

import java.util.Random;

public class TestUtils {
	private static final Random RANDOM = new Random();

	private TestUtils() {
		throw new AssertionError();
	}

	public static CommitId getCommitId(int seed) {
		byte[] bytes = new byte[32];
		RANDOM.setSeed(seed);
		RANDOM.nextBytes(bytes);
		return CommitId.ofBytes(bytes);
	}

	public static StructuredCodec<TestOp> getCodec() {
		return StructuredCodec.of(
				in -> {
					switch (in.readString()) {
						case "add":
							return new TestAdd(in.readInt());
						case "set":
							return new TestSet(in.readInt(), in.readInt());
						default:
							throw new UnsupportedOperationException();
					}
				}, (out, item) -> {
					if (item instanceof TestAdd) {
						TestAdd addOp = (TestAdd) item;
						out.writeString("add");
						out.writeInt(addOp.getDelta());
					} else if (item instanceof TestSet) {
						TestSet setOp = (TestSet) item;
						out.writeString("set");
						out.writeInt(setOp.getPrev());
						out.writeInt(setOp.getNext());
					} else {
						throw new UnsupportedOperationException();
					}
				}
		);
	}

}
