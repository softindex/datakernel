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

package io.global.common;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import org.spongycastle.crypto.CryptoException;

import java.util.Arrays;

import static io.global.ot.util.BinaryDataFormats.*;

public final class SharedSimKey implements ByteArrayIdentity {
	private final byte[] bytes;

	private final Hash hash;
	private final byte[] encrypted;

	private SharedSimKey(byte[] bytes, Hash hash, byte[] encrypted) {
		this.bytes = bytes;
		this.hash = hash;
		this.encrypted = encrypted;
	}

	public static SharedSimKey ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		return new SharedSimKey(bytes, Hash.ofBytes(readBytes(buf)), readBytes(buf));
	}

	public static SharedSimKey of(Hash hash, byte[] encryptedSimKey) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(hash.toBytes()) + sizeof(encryptedSimKey));
		write(buf, hash);
		writeBytes(buf, encryptedSimKey);
		return new SharedSimKey(buf.asArray(), hash, encryptedSimKey);
	}

	public static SharedSimKey of(SimKey simKey, PubKey receiver) {
		return of(Hash.of(simKey), receiver.encrypt(simKey));
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public Hash getHash() {
		return hash;
	}

	public byte[] getEncrypted() {
		return encrypted;
	}

	public SimKey decryptSimKey(PrivKey privKey) throws CryptoException {
		return SimKey.ofBytes(privKey.decrypt(encrypted));
	}

	@Override
	public int hashCode() {
		return 31 * hash.hashCode() + Arrays.hashCode(encrypted);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SharedSimKey that = (SharedSimKey) o;

		return hash.equals(that.hash) && Arrays.equals(encrypted, that.encrypted);
	}

	@Override
	public String toString() {
		return "SharedSimKey{hash=" + hash + ", encrypted=@" + Integer.toHexString(Arrays.hashCode(encrypted)) + '}';
	}
}
