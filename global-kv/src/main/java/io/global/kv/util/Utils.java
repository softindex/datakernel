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
import io.datakernel.exception.ParseException;
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
		byte[] value = rawKvItem.getValue();
		long timestamp = rawKvItem.getTimestamp();
		Hash hash = rawKvItem.getSimKeyHash();
		Signature signature = signedDbItem.getSignature();
		byte[] r = signature.getR().toByteArray();
		byte[] s = signature.getS().toByteArray();

		ByteBuf buf = ByteBufPool.allocate(5 + value.length + 4 + 5 + (hash == null ? 1 : 1 + hash.getBytes().length) + 5 + r.length + 5 + s.length);

		buf.write(value);
		buf.writeLong(timestamp);
		if (hash != null) {
			buf.writeByte((byte) 1);
			buf.writeVarInt(hash.getBytes().length);
			buf.write(hash.getBytes());
		} else {
			buf.writeByte((byte) 0);
		}
		buf.writeVarInt(r.length);
		buf.write(r);
		buf.writeVarInt(s.length);
		buf.write(s);
		return buf.asArray();
	}

	public static SignedData<RawKvItem> unpackValue(byte[] key, byte[] value) throws ParseException {
		if (value == null) {
			return null;
		}
		ByteBuf buf = ByteBuf.wrapForReading(value);

		byte[] theValue = new byte[buf.readVarInt()];
		buf.read(theValue);
		long timestamp = buf.readLong();
		Hash simKeyHash = null;
		if (buf.readByte() == 1) {
			byte[] h = new byte[buf.readVarInt()];
			buf.read(h);
			simKeyHash = Hash.of(h);
		}
		byte[] r = new byte[buf.readByte()];
		buf.read(r);
		byte[] s = new byte[buf.readByte()];
		buf.read(s);

		Signature signature = Signature.parse(new BigInteger(r), new BigInteger(s));
		RawKvItem newRawKvItem = RawKvItem.parse(key, value, timestamp, simKeyHash);
		return SignedData.parse(REGISTRY.get(RawKvItem.class), newRawKvItem, signature);
	}
}
