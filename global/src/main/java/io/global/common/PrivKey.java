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

import io.datakernel.exception.ParseException;
import org.spongycastle.crypto.CryptoException;
import org.spongycastle.crypto.params.ECPrivateKeyParameters;

import java.math.BigInteger;

public final class PrivKey {
	private final ECPrivateKeyParameters ecPrivateKey;

	// region creators
	public PrivKey(ECPrivateKeyParameters ecPrivateKey) {
		this.ecPrivateKey = ecPrivateKey;
	}

	public static PrivKey of(BigInteger d) {
		return new PrivKey(new ECPrivateKeyParameters(d, CryptoUtils.CURVE));
	}

	public static PrivKey parse(BigInteger d) throws ParseException {
		try {
			return PrivKey.of(d);
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(BinaryDataFormats.class, "Failed to read private key", e);
		}
	}

	public static PrivKey fromString(String repr) throws ParseException {
		try {
			return of(new BigInteger(repr, 16));
		} catch (NumberFormatException e) {
			throw new ParseException(PubKey.class, "Failed to parse big integer", e);
		}
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

	public byte[] decrypt(byte[] encrypted) throws CryptoException {
		return CryptoUtils.decryptECIES(encrypted, ecPrivateKey);
	}

	public String asString() {
		return ecPrivateKey.getD().toString(16);
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
}
