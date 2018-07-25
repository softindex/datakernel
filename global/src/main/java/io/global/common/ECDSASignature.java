package io.global.common;

import java.math.BigInteger;

public final class ECDSASignature {
	public final BigInteger r, s;

	public ECDSASignature(BigInteger r, BigInteger s) {
		this.r = r;
		this.s = s;
	}

	public boolean isCanonical() {
		return s.compareTo(CryptoUtils.HALF_CURVE_ORDER) <= 0;
	}

	public ECDSASignature toCanonicalised() {
		if (!isCanonical()) {
			return new ECDSASignature(r, CryptoUtils.CURVE.getN().subtract(s));
		} else {
			return this;
		}
	}
}
