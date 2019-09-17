package io.global.kv.util;

import io.datakernel.exception.ParseException;
import io.global.common.KeyPair;
import io.global.common.SignedData;
import io.global.kv.api.RawKvItem;
import org.junit.Test;

import static io.global.kv.util.BinaryDataFormats.RAW_KV_ITEM_CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class UtilsTest {
	@Test
	public void testPackUnpack() throws ParseException {
		byte[] key = "key".getBytes(UTF_8);
		byte[] value = "value".getBytes(UTF_8);
		RawKvItem rawKvItem = RawKvItem.of(key, value, 100);
		KeyPair keys = KeyPair.generate();
		SignedData<RawKvItem> before = SignedData.sign(RAW_KV_ITEM_CODEC, rawKvItem, keys.getPrivKey());

		byte[] bytes = Utils.packValue(before);
		SignedData<RawKvItem> after = Utils.unpackValue(key, bytes);
		assertEquals(before, after);
		assertTrue(before.verify(keys.getPubKey()));
	}
}
