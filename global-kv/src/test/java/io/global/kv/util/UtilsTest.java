package io.global.kv.util;

import io.datakernel.common.parse.ParseException;
import io.datakernel.test.rules.ByteBufRule;
import io.global.common.Hash;
import io.global.common.KeyPair;
import io.global.common.SignedData;
import io.global.kv.api.RawKvItem;
import org.junit.ClassRule;
import org.junit.Test;

import static io.global.kv.util.BinaryDataFormats.RAW_KV_ITEM_CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class UtilsTest {
	private static final KeyPair KEY_PAIR = KeyPair.generate();
	private static final byte[] KEY = "key".getBytes(UTF_8);

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testPackUnpackNoHash() throws ParseException {
		doTest(RawKvItem.of(KEY, "value".getBytes(UTF_8), System.currentTimeMillis()));
	}

	@Test
	public void testPackUnpackWithHash() throws ParseException {
		doTest(RawKvItem.parse(KEY, "value".getBytes(UTF_8), System.currentTimeMillis(), Hash.sha1(new byte[]{1, 2, 3})));
	}

	@Test
	public void testPackUnpackTombstone() throws ParseException {
		doTest(RawKvItem.tombstone(KEY, System.currentTimeMillis()));
	}

	private void doTest(RawKvItem item) throws ParseException {
		SignedData<RawKvItem> before = SignedData.sign(RAW_KV_ITEM_CODEC, item, KEY_PAIR.getPrivKey());
		byte[] bytes = Utils.packValue(before);
		SignedData<RawKvItem> after = Utils.unpackValue(KEY, bytes);
		assertEquals(before, after);
		assertTrue(before.verify(KEY_PAIR.getPubKey()));

	}
}
