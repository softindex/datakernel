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

package io.global.db.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.global.common.Signature;
import io.global.common.SignedData;
import io.global.db.Blob;
import io.global.db.DbItem;

import java.math.BigInteger;

import static io.datakernel.serializer.util.BinaryOutputUtils.writeByte;
import static io.datakernel.serializer.util.BinaryOutputUtils.writeVarInt;
import static io.global.db.util.BinaryDataFormats.REGISTRY;
import static java.lang.System.arraycopy;

public final class Utils {
	private Utils() {
		throw new AssertionError("nope.");
	}

	private static int getVarIntLength(int v) {
		if ((v & ~0x7F) == 0) return 1;
		if (((v >>>= 7) & ~0x7F) == 0) return 2;
		if (((v >>>= 7) & ~0x7F) == 0) return 3;
		if ((v >>> 7 & ~0x7F) == 0) return 4;
		else return 5;
	}

	public static DbItem convert(SignedData<DbItem> signedDbItem) {
		Signature signature = signedDbItem.getSignature();
		byte[] r = signature.getR().toByteArray();
		byte[] s = signature.getS().toByteArray();

		DbItem dbItem = signedDbItem.getValue();
		Blob blob = dbItem.getValue();
		byte[] value = blob.getData();

		int varIntLength = getVarIntLength(value.length);
		byte[] newValue = new byte[varIntLength + value.length + 1 + r.length + 1 + s.length];

		writeVarInt(newValue, 0, value.length);
		arraycopy(value, 0, newValue, varIntLength, value.length);
		writeByte(newValue, varIntLength + value.length, (byte) r.length);
		arraycopy(r, 0, newValue, varIntLength + value.length + 1, r.length);
		writeByte(newValue, varIntLength + value.length + 1 + r.length, (byte) s.length);
		arraycopy(s, 0, newValue, varIntLength + value.length + 1 + r.length + 1, s.length);

		return DbItem.of(dbItem.getKey(), Blob.of(blob.getTimestamp(), newValue));
	}

	public static SignedData<DbItem> convert(DbItem dbItem) throws ParseException {
		byte[] container = dbItem.getValue().getData();
		ByteBuf buf = ByteBuf.wrapForReading(container);

		byte[] data = new byte[buf.readVarInt()];
		buf.read(data);
		byte[] r = new byte[buf.readByte()];
		buf.read(r);
		byte[] s = new byte[buf.readByte()];
		buf.read(s);

		Signature signature = Signature.parse(new BigInteger(r), new BigInteger(s));
		DbItem newDbItem = DbItem.of(dbItem.getKey(), Blob.of(dbItem.getValue().getTimestamp(), data));
		return SignedData.parse(REGISTRY.get(DbItem.class), newDbItem, signature);
	}
}
