package io.global.ot.api;

import io.datakernel.common.parse.ParseException;
import org.junit.Test;

import static io.global.ot.util.TestUtils.getCommitId;
import static org.junit.Assert.assertEquals;

public class CommitIdTest {

	@Test
	public void testSerialization() throws ParseException {
		doTestSerialization(getCommitId(1));
		doTestSerialization(getCommitId(Long.MAX_VALUE, 1));
		doTestSerialization(CommitId.ofRoot());
	}

	private static void doTestSerialization(CommitId original) throws ParseException {
		byte[] bytes = original.toBytes();
		CommitId restored = CommitId.parse(bytes);

		assertEquals(original, restored);
	}
}
