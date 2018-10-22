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

	private final PubKey receiver;
	private final Hash hash;
	private final byte[] encryptedSimKey;

	private SharedSimKey(byte[] bytes, PubKey receiver, Hash hash, byte[] encryptedSimKey) {
		this.bytes = bytes;
		this.receiver = receiver;
		this.hash = hash;
		this.encryptedSimKey = encryptedSimKey;
	}

	public static SharedSimKey ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);

		PubKey receiver = readPubKey(buf);
		Hash hash = Hash.ofBytes(readBytes(buf));
		byte[] encryptedSimKey = readBytes(buf);

		return new SharedSimKey(bytes, receiver, hash, encryptedSimKey);
	}

	public static SharedSimKey of(PubKey receiver, Hash hash, byte[] encryptedSimKey) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(receiver) + sizeof(hash.toBytes()) + sizeof(encryptedSimKey));

		writePubKey(buf, receiver);
		write(buf, hash);
		writeBytes(buf, encryptedSimKey);

		return new SharedSimKey(buf.asArray(), receiver, hash, encryptedSimKey);
	}

	public static SharedSimKey of(PubKey receiver, SimKey simKey) {
		return of(receiver, Hash.of(simKey), receiver.encrypt(simKey));
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public PubKey getReceiver() {
		return receiver;
	}

	public Hash getHash() {
		return hash;
	}

	public byte[] getEncryptedSimKey() {
		return encryptedSimKey;
	}

	public SimKey decryptSimKey(PrivKey privKey) throws CryptoException {
		return SimKey.ofBytes(privKey.decrypt(encryptedSimKey));
	}

	@Override
	public int hashCode() {
		return 31 * (31 * receiver.hashCode() + hash.hashCode()) + Arrays.hashCode(encryptedSimKey);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SharedSimKey that = (SharedSimKey) o;

		return receiver.equals(that.receiver) && hash.equals(that.hash) && Arrays.equals(encryptedSimKey, that.encryptedSimKey);
	}

	@Override
	public String toString() {
		return "SharedSimKey{receiver=" + receiver + ", hash=" + hash + ", encryptedSimKey=@" + Integer.toHexString(Arrays.hashCode(encryptedSimKey)) + '}';
	}
}
