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

package io.global.kv.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.parse.ParseException;
import io.global.common.Hash;
import io.global.common.Signature;
import io.global.common.SignedData;
import io.global.kv.api.RawKvItem;

import java.math.BigInteger;

import static io.global.kv.util.BinaryDataFormats.REGISTRY;

public final class Utils {
	private Utils() {}

	public static byte[] packValue(SignedData<RawKvItem> signedDbItem) {
		RawKvItem rawKvItem = signedDbItem.getValue();
		long timestamp = rawKvItem.getTimestamp();
		Hash hash = rawKvItem.getSimKeyHash();
		Signature signature = signedDbItem.getSignature();
		byte[] r = signature.getR().toByteArray();
		byte[] s = signature.getS().toByteArray();

		boolean valuePresent = !rawKvItem.isTombstone();
		ByteBuf buf = ByteBufPool.allocate(9 +
				(!valuePresent ? 1 : (1 + 5 + rawKvItem.getValue().length +
						(hash == null ? 1 : 1 + hash.getBytes().length))) +
				5 + r.length + 5 + s.length);

		buf.writeVarLong(timestamp);
		buf.writeBoolean(valuePresent);
		if (valuePresent) {
			byte[] value = rawKvItem.getValue();
			buf.writeVarInt(value.length);
			buf.write(value);
			buf.writeBoolean(hash != null);
			if (hash != null) {
				buf.writeVarInt(hash.getBytes().length);
				buf.write(hash.getBytes());
			}
		}
		buf.writeVarInt(r.length);
		buf.write(r);
		buf.writeVarInt(s.length);
		buf.write(s);
		return buf.asArray();
	}

	public static SignedData<RawKvItem> unpackValue(byte[] key, byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		long timestamp = buf.readVarLong();
		byte[] value = null;
		Hash hash = null;
		if (buf.readBoolean()){
			value = new byte[buf.readVarInt()];
			buf.read(value);
			if (buf.readBoolean()){
				byte[] h = new byte[buf.readVarInt()];
				buf.read(h);
				hash = Hash.of(h);
			}
		}
		byte[] r = new byte[buf.readByte()];
		buf.read(r);
		byte[] s = new byte[buf.readByte()];
		buf.read(s);

		Signature signature = Signature.parse(new BigInteger(r), new BigInteger(s));
		RawKvItem newRawKvItem = RawKvItem.parse(key, value, timestamp, hash);
		return SignedData.parse(REGISTRY.get(RawKvItem.class), newRawKvItem, signature);
	}
}
