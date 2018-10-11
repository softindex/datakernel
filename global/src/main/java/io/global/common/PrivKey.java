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

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.global.globalsync.util.BinaryDataFormats;
import org.spongycastle.crypto.params.ECPrivateKeyParameters;

import java.math.BigInteger;
import java.util.Base64;

import static io.global.globalsync.util.BinaryDataFormats.sizeof;
import static io.global.globalsync.util.BinaryDataFormats.writePrivKey;

public final class PrivKey {
	private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
	private static final Base64.Decoder decoder = Base64.getUrlDecoder();

	private final ECPrivateKeyParameters ecPrivateKey;

	// region creators
	public PrivKey(ECPrivateKeyParameters ecPrivateKey) {
		this.ecPrivateKey = ecPrivateKey;
	}

	public static PrivKey ofD(BigInteger d) {
		return new PrivKey(new ECPrivateKeyParameters(d, CryptoUtils.CURVE));
	}

	@Nullable
	public static PrivKey fromString(@Nullable String repr) throws ParseException {
		if (repr == null) {
			return null;
		}
		return BinaryDataFormats.readPrivKey(ByteBuf.wrapForReading(decoder.decode(repr)));
	}
	// endregion

	public ECPrivateKeyParameters getEcPrivateKey() {
		return ecPrivateKey;
	}

	public PubKey computePubKey() {
		return new PubKey(CryptoUtils.computePubKey(ecPrivateKey));
	}

	public KeyPair computeKeys() {
		return new KeyPair(this, computePubKey());
	}

	public String asString() {
		byte[] bytes = new byte[sizeof(this)];
		writePrivKey(ByteBuf.wrapForWriting(bytes), this);
		return encoder.encodeToString(bytes);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PrivKey privKey = (PrivKey) o;

		return ecPrivateKey.getD().equals(privKey.ecPrivateKey.getD());
	}

	@Override
	public int hashCode() {
		return ecPrivateKey.getD().hashCode();
	}

	@Override
	public String toString() {
		return "PrivKey@" + Integer.toHexString(hashCode());
	}

	public static void main(String[] args) {
		System.out.println(KeyPair.generate().getPrivKey().asString());
	}
}
