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
import io.global.ot.util.BinaryDataFormats2;
import org.spongycastle.crypto.params.ECPublicKeyParameters;
import org.spongycastle.math.ec.ECPoint;

import java.math.BigInteger;

public final class PubKey {
	private final ECPublicKeyParameters ecPublicKey;

	// region creators
	public PubKey(ECPublicKeyParameters ecPublicKey) {
		this.ecPublicKey = ecPublicKey;
	}

	public static PubKey of(ECPoint q) {
		return new PubKey(new ECPublicKeyParameters(q, CryptoUtils.CURVE));
	}

	public static PubKey parse(ECPoint q) throws ParseException {
		try {
			return PubKey.of(q);
		} catch (IllegalArgumentException | ArithmeticException e) {
			throw new ParseException(BinaryDataFormats2.class, "Failed to read public key", e);
		}
	}

	public static PubKey fromString(String string) throws ParseException {
		String[] parts = string.split(":");
		if (parts.length != 2) {
			throw new ParseException(PubKey.class, "No or more than one ':' delimiters in public key string");
		}
		try {
			BigInteger x = new BigInteger(parts[0], 16);
			BigInteger y = new BigInteger(parts[1], 16);
			try {
				return PubKey.of(CryptoUtils.CURVE.getCurve().validatePoint(x, y));
			} catch (IllegalArgumentException | ArithmeticException e) {
				throw new ParseException(PubKey.class, "Failed to read a point on elliptic curve", e);
			}
		} catch (NumberFormatException e) {
			throw new ParseException(PubKey.class, "Failed to parse big integer", e);
		}
	}
	// endregion

	public ECPublicKeyParameters getEcPublicKey() {
		return ecPublicKey;
	}

	public byte[] encrypt(byte[] data) {
		return CryptoUtils.encryptECIES(data, ecPublicKey);
	}

	public String asString() {
		ECPoint q = ecPublicKey.getQ();
		return q.getXCoord() + ":" + q.getYCoord();
	}

	@Override
	public int hashCode() {
		return ecPublicKey.getQ().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PubKey pubKey = (PubKey) o;
		return ecPublicKey.getQ().equals(pubKey.ecPublicKey.getQ());
	}

	@Override
	public String toString() {
		return "PubKey@" + Integer.toHexString(hashCode());
	}
}
