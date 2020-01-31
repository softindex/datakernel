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

import io.datakernel.common.parse.ParseException;

import java.math.BigInteger;

public final class Signature {
	private final BigInteger r;
	private final BigInteger s;

	// region creators
	private Signature(BigInteger r, BigInteger s) {
		this.r = r;
		this.s = s;
	}

	public static Signature of(BigInteger r, BigInteger s) {
		return new Signature(r, s);
	}

	public static Signature parse(BigInteger r, BigInteger s) throws ParseException {
		return new Signature(r, s); // TODO
	}

	public static Signature fromString(String string) throws ParseException {
		String[] parts = string.split(":");
		if (parts.length != 2) {
			throw new ParseException(PubKey.class, "No ':' delimiter in public key string");
		}
		try {
			return new Signature(new BigInteger(parts[0], 16), new BigInteger(parts[1], 16));
		} catch (NumberFormatException e) {
			throw new ParseException(PubKey.class, "Failed to parse big integer", e);
		}
	}
	// endregion

	public boolean isCanonical() {
		return s.compareTo(CryptoUtils.HALF_CURVE_ORDER) <= 0;
	}

	public Signature toCanonicalised() {
		if (!isCanonical()) {
			return new Signature(r, CryptoUtils.CURVE.getN().subtract(s));
		} else {
			return this;
		}
	}

	public BigInteger getR() {
		return r;
	}

	public BigInteger getS() {
		return s;
	}

	public String asString() {
		return r.toString(16) + ":" + s.toString(16);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Signature that = (Signature) o;
		return r.equals(that.r) && s.equals(that.s);
	}

	@Override
	public int hashCode() {
		return 31 * r.hashCode() + s.hashCode();
	}

	@Override
	public String toString() {
		return "Signature{r=" + r + ", s=" + s + '}';
	}
}
